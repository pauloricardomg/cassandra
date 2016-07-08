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

package org.apache.cassandra.repair.mutation;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowDiffListener;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.net.AsyncOneResponse;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.DataResolver;
import org.apache.cassandra.service.RepairMergeListener;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.FBUtilities;


/**
 * Callback for the RR sent to replicas during mutation based repair
 *
 * We use an internal response thread to receive the message, but heavy lifting (applying mutations) is offloaded to
 * another thread. MBRService will block until the count down latch is 0, and this means that
 * it will block until the repair page has been written to the memtable.
 */
public class MBRResponseCallback implements IAsyncCallback<MBRResponse>
{
    private static final ExecutorService executor = new JMXEnabledThreadPoolExecutor(1,
                                                                                     StageManager.KEEPALIVE,
                                                                                     TimeUnit.SECONDS,
                                                                                     new LinkedBlockingQueue<>(),
                                                                                     new NamedThreadFactory("MutationRepairMutations"),
                                                                                     "internal");
    private static final Logger logger = LoggerFactory.getLogger(MBRResponseCallback.class);

    private final MBRRepairPage repairPage;
    private final ColumnFamilyStore cfs;
    private final int nowInSeconds;
    private final CountDownLatch cdl;
    private final Set<MessageIn<MBRResponse>> responses = new CopyOnWriteArraySet<>();
    private final AtomicInteger requiredResponses;
    private final MBRMetricHolder metrics;

    public MBRResponseCallback(ColumnFamilyStore cfs, MBRRepairPage rp, int nowInSeconds, CountDownLatch cdl, int expectedResponses, MBRMetricHolder metrics)
    {
        repairPage = rp;
        this.cfs = cfs;
        this.nowInSeconds = nowInSeconds;
        this.cdl = cdl;
        this.metrics = metrics;
        this.requiredResponses = new AtomicInteger(expectedResponses);
    }

    public void response(MessageIn<MBRResponse> msg)
    {
        responses.add(msg);
        if (requiredResponses.decrementAndGet() == 0)
        {
            executor.submit(() ->
                            {
                                resolveDiff(repairPage, responses.stream().filter(m -> m.payload.type != MBRResponse.Type.MATCH).collect(Collectors.toSet()));
                                cdl.countDown();
                            });
        }
        else
        {
            cdl.countDown();
        }
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }


    private void resolveDiff(MBRRepairPage repairPage, Set<MessageIn<MBRResponse>> responses)
    {
        List<UnfilteredPartitionIterator> remoteIterators = new ArrayList<>(responses.size());
        List<InetAddress> hugeResponses = new ArrayList<>(responses.size());
        ReadCommand rc = repairPage.createReadCommand(cfs, nowInSeconds, Integer.MAX_VALUE);
        try
        {
            for (MessageIn<MBRResponse> response : responses)
            {
                switch (response.payload.type)
                {
                    case DATA:
                        UnfilteredPartitionIterator it = response.payload.response.makeIterator(rc);
                        remoteIterators.add(it);
                        break;
                    case HUGE:
                        hugeResponses.add(response.from);
                        break;
                    case MATCH:
                        assert false : "We should not see MATCH types here";
                        break;
                }
            }

            if (hugeResponses.isEmpty() && remoteIterators.isEmpty())
            {
                logger.trace("all matching");
            }
            else if (!hugeResponses.isEmpty())
            {
                metrics.hugePage();
                handleHugeResponses(rc); // we also need to read the data from the diffing nodes
            }
            else if (!remoteIterators.isEmpty())
            {
                metrics.mismatchedPage();
                try (ReadExecutionController rec = rc.executionController();
                     UnfilteredPartitionIterator existingData = rc.executeLocally(rec))
                {
                    List<UnfilteredPartitionIterator> allIterators = Lists.newArrayList();
                    allIterators.addAll(remoteIterators);
                    if (existingData.hasNext())
                    {
                        allIterators.add(0, existingData);
                        List<InetAddress> sources = new ArrayList<>();
                        sources.add(FBUtilities.getBroadcastAddress());
                        sources.addAll(responses.stream().map(r -> r.from).collect(Collectors.toList()));
                        try (UnfilteredPartitionIterator it = UnfilteredPartitionIterators.merge(allIterators, nowInSeconds, new MBRMergeListener(cfs.keyspace, sources.toArray(new InetAddress[sources.size()]), rc, ConsistencyLevel.ALL)))
                        {
                            while (it.hasNext())
                                try (UnfilteredRowIterator ri = it.next())
                                {
                                    // the data gets written in the merger so that we only write a diff between remote and local data
                                    while (ri.hasNext())
                                        ri.next();
                                }
                        }
                    }
                    else
                    {
                        try (UnfilteredPartitionIterator it = UnfilteredPartitionIterators.merge(allIterators, nowInSeconds, new EmptyMergeListener()))
                        {
                            while (it.hasNext())
                            {
                                try (UnfilteredRowIterator ri = it.next())
                                {
                                    PartitionUpdate pu = PartitionUpdate.fromIterator(ri, ColumnFilter.all(cfs.metadata));
                                    Mutation m = new Mutation(pu);
                                    m.apply();
                                    metrics.appliedMutation();
                                }
                            }
                        }
                    }
                }
            }
        }
        finally
        {
            try
            {
                FBUtilities.closeAll(remoteIterators);
            }
            catch (Throwable t)
            {
                logger.error("Got exception closing remote iterators", t);
                throw new RuntimeException(t);
            }
            finally
            {
                cdl.countDown();
            }
        }
    }

    /**
     * We have atleast one remote node where a remote node has many more rows within the given range
     *
     * So we have these cases;
     *
     * * All remote node has many more rows
     *   - we need to read from all remote nodes
     * * A single remote node has many more rows and the others just small diffs
     *   - we need to read the ranges from all nodes
     * * A single remote node has many more rows, all others match
     *   - we only *need* to read from the one with the big diff, but reading a single window of data
     *     from the matching nodes will not hurt that much. Could be a future improvement to be able to
     *     filter out the nodes where we know the data matches
     *
     *     // TODO: fall back to lower CL if all nodes are not up
     */
    private void handleHugeResponses(ReadCommand rc)
    {
        QueryPager pager = rc.getPager(null, Server.CURRENT_VERSION);
        while (!pager.isExhausted())
        {
            try (UnfilteredPartitionIterator pi = pager.fetchUnfilteredPage(repairPage.windowSize, ConsistencyLevel.ALL, ClientState.forInternalCalls(), rc.metadata()))
            {
                while (pi.hasNext())
                {
                    try (UnfilteredRowIterator ri = pi.next())
                    {
                        metrics.appliedHugeResponseMutation();
                        // todo: throttle this!
                        PartitionUpdate pu = PartitionUpdate.fromIterator(ri, ColumnFilter.all(cfs.metadata));
                        Mutation m = new Mutation(pu);
                        m.apply();
                    }
                }
            }
        }
    }

    public Set<InetAddress> replies()
    {
        return responses.stream().map(response -> response.from).collect(Collectors.toSet());
    }

    private static class MBRMergeListener extends RepairMergeListener
    {
        public MBRMergeListener(Keyspace keyspace, InetAddress[] sources, ReadCommand command, ConsistencyLevel consistency)
        {
            super(keyspace, sources, command, consistency);
        }

        public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
        {
            return new MBRDiffListener(partitionKey, columns(versions), isReversed(versions), sources, command, repairResults);
        }
    }

    private static class MBRDiffListener extends RepairMergeListener.MergeListener
    {

        public MBRDiffListener(DecoratedKey partitionKey, PartitionColumns columns, boolean isReversed, InetAddress[] sources, ReadCommand command, List<AsyncOneResponse> repairResults)
        {
            super(partitionKey, columns, isReversed, sources, command, repairResults);
        }

        public void close()
        {
            for (int i = 0; i < repairs.length; i++)
            {
                if (repairs[i] == null)
                    continue;

                // use a separate verb here because we don't want these to be get the white glove hint-
                // on-timeout behavior that a "real" mutation gets
                Tracing.trace("Sending repair-mutation to {}", sources[i]);
                logger.info("sending {} mutations to {}", repairs[i].operationCount(), sources[i]);
                MessageOut<Mutation> msg = new Mutation(repairs[i]).createMessage(MessagingService.Verb.READ_REPAIR);
                repairResults.add(MessagingService.instance().sendRR(msg, sources[i]));
            }
        }
    }


    private static class EmptyMergeListener implements UnfilteredPartitionIterators.MergeListener
    {
        public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
        {
            return new UnfilteredRowIterators.MergeListener()
            {
                public void onMergedPartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions)
                {
                }

                public void onMergedRows(Row merged, Row[] versions)
                {
                }

                public void onMergedRangeTombstoneMarkers(RangeTombstoneMarker merged, RangeTombstoneMarker[] versions)
                {
                }

                public void close()
                {
                }
            };
        }

        public void close()
        {
        }
    }
}
