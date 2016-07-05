/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.MoreRows;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.net.*;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class DataResolver extends ResponseResolver
{
    public DataResolver(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistency, int maxResponseCount)
    {
        super(keyspace, command, consistency, maxResponseCount);
    }

    public PartitionIterator getData()
    {
        return UnfilteredPartitionIterators.filter(getUnfilteredData(), command.nowInSec());
    }

    public UnfilteredPartitionIterator getUnfilteredData()
    {
        ReadResponse response = responses.iterator().next().payload;
        return response.makeIterator(command);
    }

    public PartitionIterator resolve()
    {
        Pair<List<UnfilteredPartitionIterator>, InetAddress[]> sources = getSources();
        // Even though every responses should honor the limit, we might have more than requested post reconciliation,
        // so ensure we're respecting the limit.
        DataLimits.Counter counter = command.limits().newCounter(command.nowInSec(), true);
        return counter.applyTo(UnfilteredPartitionIterators.filter(mergeWithShortReadProtection(sources.left, sources.right, counter), command.nowInSec()));
    }

    public UnfilteredPartitionIterator resolveUnfiltered()
    {
        DataLimits.Counter counter = command.limits().newCounter(command.nowInSec(), true);
        Pair<List<UnfilteredPartitionIterator>, InetAddress[]> sources = getSources();
        // TODO: short read protection and read repair?
        return counter.applyTo(UnfilteredPartitionIterators.mergeLazily(sources.left, command.nowInSec()));
    }

    private Pair<List<UnfilteredPartitionIterator>, InetAddress[]> getSources()
    {
        // We could get more responses while this method runs, which is ok (we're happy to ignore any response not here
        // at the beginning of this method), so grab the response count once and use that through the method.

        // Even though every responses should honor the limit, we might have more than requested post reconciliation,
        // so ensure we're respecting the limit.
        int count = responses.size();
        List<UnfilteredPartitionIterator> iters = new ArrayList<>(count);
        InetAddress[] sources = new InetAddress[count];
        for (int i = 0; i < count; i++)
        {
            MessageIn<ReadResponse> msg = responses.get(i);
            iters.add(msg.payload.makeIterator(command));
            sources[i] = msg.from;
        }
        return Pair.create(iters, sources);
    }

    private UnfilteredPartitionIterator mergeWithShortReadProtection(List<UnfilteredPartitionIterator> results, InetAddress[] sources, DataLimits.Counter resultCounter)
    {
        // If we have only one results, there is no read repair to do and we can't get short reads
        if (results.size() == 1)
            return results.get(0);

        UnfilteredPartitionIterators.MergeListener listener = new RepairMergeListener(keyspace, sources, command, consistency);

        // So-called "short reads" stems from nodes returning only a subset of the results they have for a partition due to the limit,
        // but that subset not being enough post-reconciliation. So if we don't have limit, don't bother.
        if (!command.limits().isUnlimited())
        {
            for (int i = 0; i < results.size(); i++)
                results.set(i, Transformation.apply(results.get(i), new ShortReadProtection(sources[i], resultCounter)));
        }
        // todo: before we did mergeAndFilter, now we first merge, then filter, this is slower
        return UnfilteredPartitionIterators.merge(results, command.nowInSec(), listener);
    }

    private class ShortReadProtection extends Transformation<UnfilteredRowIterator>
    {
        private final InetAddress source;
        private final DataLimits.Counter counter;
        private final DataLimits.Counter postReconciliationCounter;

        private ShortReadProtection(InetAddress source, DataLimits.Counter postReconciliationCounter)
        {
            this.source = source;
            this.counter = command.limits().newCounter(command.nowInSec(), false).onlyCount();
            this.postReconciliationCounter = postReconciliationCounter;
        }

        @Override
        public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            partition = Transformation.apply(partition, counter);
            // must apply and extend with same protection instance
            ShortReadRowProtection protection = new ShortReadRowProtection(partition.metadata(), partition.partitionKey());
            partition = MoreRows.extend(partition, protection);
            partition = Transformation.apply(partition, protection); // apply after, so it is retained when we extend (in case we need to reextend)
            return partition;
        }

        private class ShortReadRowProtection extends Transformation implements MoreRows<UnfilteredRowIterator>
        {
            final CFMetaData metadata;
            final DecoratedKey partitionKey;
            Clustering lastClustering;
            int lastCount = 0;

            private ShortReadRowProtection(CFMetaData metadata, DecoratedKey partitionKey)
            {
                this.metadata = metadata;
                this.partitionKey = partitionKey;
            }

            @Override
            public Row applyToRow(Row row)
            {
                lastClustering = row.clustering();
                return row;
            }

            @Override
            public UnfilteredRowIterator moreContents()
            {
                // We have a short read if the node this is the result of has returned the requested number of
                // rows for that partition (i.e. it has stopped returning results due to the limit), but some of
                // those results haven't made it in the final result post-reconciliation due to other nodes
                // tombstones. If that is the case, then the node might have more results that we should fetch
                // as otherwise we might return less results than required, or results that shouldn't be returned
                // (because the node has tombstone that hides future results from other nodes but that haven't
                // been returned due to the limit).
                // Also note that we only get here once all the results for this node have been returned, and so
                // if the node had returned the requested number but we still get there, it imply some results were
                // skipped during reconciliation.
                if (lastCount == counter.counted() || !counter.isDoneForPartition())
                    return null;
                lastCount = counter.counted();

                assert !postReconciliationCounter.isDoneForPartition();

                // We need to try to query enough additional results to fulfill our query, but because we could still
                // get short reads on that additional query, just querying the number of results we miss may not be
                // enough. But we know that when this node answered n rows (counter.countedInCurrentPartition), only
                // x rows (postReconciliationCounter.countedInCurrentPartition()) made it in the final result.
                // So our ratio of live rows to requested rows is x/n, so since we miss n-x rows, we estimate that
                // we should request m rows so that m * x/n = n-x, that is m = (n^2/x) - n.
                // Also note that it's ok if we retrieve more results that necessary since our top level iterator is a
                // counting iterator.
                int n = postReconciliationCounter.countedInCurrentPartition();
                int x = counter.countedInCurrentPartition();
                int toQuery = Math.max(((n * n) / x) - n, 1);

                DataLimits retryLimits = command.limits().forShortReadRetry(toQuery);
                ClusteringIndexFilter filter = command.clusteringIndexFilter(partitionKey);
                ClusteringIndexFilter retryFilter = lastClustering == null ? filter : filter.forPaging(metadata.comparator, lastClustering, false);
                SinglePartitionReadCommand cmd = SinglePartitionReadCommand.create(command.metadata(),
                                                                                   command.nowInSec(),
                                                                                   command.columnFilter(),
                                                                                   command.rowFilter(),
                                                                                   retryLimits,
                                                                                   partitionKey,
                                                                                   retryFilter);

                return doShortReadRetry(cmd);
            }

            private UnfilteredRowIterator doShortReadRetry(SinglePartitionReadCommand retryCommand)
            {
                DataResolver resolver = new DataResolver(keyspace, retryCommand, ConsistencyLevel.ONE, 1);
                ReadCallback handler = new ReadCallback(resolver, ConsistencyLevel.ONE, retryCommand, Collections.singletonList(source));
                if (StorageProxy.canDoLocalRequest(source))
                      StageManager.getStage(Stage.READ).maybeExecuteImmediately(new StorageProxy.LocalReadRunnable(retryCommand, handler));
                else
                    MessagingService.instance().sendRRWithFailure(retryCommand.createMessage(MessagingService.current_version), source, handler);

                // We don't call handler.get() because we want to preserve tombstones since we're still in the middle of merging node results.
                handler.awaitResults();
                assert resolver.responses.size() == 1;
                return UnfilteredPartitionIterators.getOnlyElement(resolver.responses.get(0).payload.makeIterator(command), retryCommand);
            }
        }
    }

    public boolean isDataPresent()
    {
        return !responses.isEmpty();
    }
}
