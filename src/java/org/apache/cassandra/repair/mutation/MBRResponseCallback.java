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
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.FBUtilities;

public class MBRResponseCallback implements IAsyncCallback<MBRResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(MBRResponseCallback.class);

    private final MBRRepairPage repairPage;
    private final ColumnFamilyStore cfs;
    private final int nowInSeconds;
    private final CountDownLatch cdl;
    private final int expectedResponses;
    private final Set<MessageIn<MBRResponse>> responses = new CopyOnWriteArraySet<>();
    private final MBRMetricHolder metrics;

    public MBRResponseCallback(ColumnFamilyStore cfs, MBRRepairPage rp, int nowInSeconds, CountDownLatch cdl, int expectedResponses, MBRMetricHolder metrics)
    {
        repairPage = rp;
        this.cfs = cfs;
        this.nowInSeconds = nowInSeconds;
        this.cdl = cdl;
        this.expectedResponses = expectedResponses;
        this.metrics = metrics;
    }

    public void response(MessageIn<MBRResponse> msg)
    {
        responses.add(msg);
        if (responses.size() == expectedResponses)
            resolveDiff(repairPage, responses);

        cdl.countDown();
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
            else
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

                        try (UnfilteredPartitionIterator it = UnfilteredPartitionIterators.merge(allIterators, nowInSeconds, new PartitionMergeListener(cfs, metrics)))
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

    // TODO: very similar to DataResolver.RepairMergeListener - refactor
    private static class PartitionMergeListener implements UnfilteredPartitionIterators.MergeListener
    {
        private final ColumnFamilyStore cfs;
        private final MBRMetricHolder metrics;

        public PartitionMergeListener(ColumnFamilyStore cfs, MBRMetricHolder metrics)
        {
            this.cfs = cfs;
            this.metrics = metrics;
        }

        public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
        {
            return new RowMerger(cfs, partitionKey, columns(versions), isReversed(versions), metrics);
        }

        private boolean isReversed(List<UnfilteredRowIterator> versions)
        {
            for (UnfilteredRowIterator iter : versions)
            {
                if (iter == null)
                    continue;

                // Everything will be in the same order
                return iter.isReverseOrder();
            }

            assert false : "Expected at least one iterator";
            return false;
        }

        private PartitionColumns columns(List<UnfilteredRowIterator> versions)
        {
            Columns statics = Columns.NONE;
            Columns regulars = Columns.NONE;
            for (UnfilteredRowIterator iter : versions)
            {
                if (iter == null)
                    continue;

                PartitionColumns cols = iter.columns();
                statics = statics.mergeTo(cols.statics);
                regulars = regulars.mergeTo(cols.regulars);
            }
            return new PartitionColumns(statics, regulars);
        }

        public void close()
        {
        }
    }

    public Set<InetAddress> replies()
    {
        return responses.stream().map(response -> response.from).collect(Collectors.toSet());
    }

    private static class RowMerger implements UnfilteredRowIterators.MergeListener
    {
        private final PartitionUpdate updater;
        private final boolean isReversed;
        private final ColumnFamilyStore cfs;
        private final DecoratedKey partitionKey;
        private final MBRMetricHolder metrics;
        private ClusteringBound markerOpen;
        private DeletionTime markerTime;
        private Row.Builder currentRow = null;

        private final RowDiffListener diffListener = new RowDiffListener()
        {
            public void onPrimaryKeyLivenessInfo(int i, Clustering clustering, LivenessInfo merged, LivenessInfo original)
            {
                if (merged != null && !merged.equals(original))
                    currentRow(clustering).addPrimaryKeyLivenessInfo(merged);
            }

            public void onDeletion(int i, Clustering clustering, Row.Deletion merged, Row.Deletion original)
            {
                if (merged != null && !merged.equals(original))
                    currentRow(clustering).addRowDeletion(merged);
            }

            public void onComplexDeletion(int i, Clustering clustering, ColumnDefinition column, DeletionTime merged, DeletionTime original)
            {
                if (merged != null && !merged.equals(original))
                    currentRow(clustering).addComplexDeletion(column, merged);
            }

            public void onCell(int i, Clustering clustering, Cell merged, Cell original)
            {
                if (merged != null && !merged.equals(original))
                    currentRow(clustering).addCell(merged);
            }

            public Row.Builder currentRow(Clustering clustering)
            {
                if (currentRow == null)
                {
                    currentRow = BTreeRow.sortedBuilder();
                    currentRow.newRow(clustering);
                }
                return currentRow;
            }
        };

        public RowMerger(ColumnFamilyStore cfs, DecoratedKey key, PartitionColumns columns, boolean isReversed, MBRMetricHolder metrics)
        {
            updater = new PartitionUpdate(cfs.metadata, key, columns, 1);
            this.isReversed = isReversed;
            this.cfs = cfs;
            this.partitionKey = key;
            this.metrics = metrics;
        }

        public void onMergedPartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions)
        {
            if (mergedDeletion.supersedes(versions[0]))
            {
                updater.addPartitionDeletion(mergedDeletion);
            }
        }

        public void onMergedRows(Row merged, Row[] versions)
        {
            if (merged.isEmpty())
                 return;

            Rows.diff(diffListener, merged, versions[0]);
            if (currentRow != null)
                 updater.add(currentRow.build());
        }

        public void onMergedRangeTombstoneMarkers(RangeTombstoneMarker merged, RangeTombstoneMarker[] versions)
        {
            RangeTombstoneMarker localMarker = versions[0];
            // Note that boundaries are both close and open, so it's not one or the other
            if (merged.isClose(isReversed) && markerOpen != null)
            {
                ClusteringBound open = markerOpen;
                ClusteringBound close = merged.closeBound(isReversed);
                updater.add(new RangeTombstone(Slice.make(isReversed ? close : open, isReversed ? open : close), markerTime));
            }
            if (merged.isOpen(isReversed) && (localMarker == null || merged.openDeletionTime(isReversed).supersedes(localMarker.openDeletionTime(isReversed))))
            {
                markerOpen = merged.openBound(isReversed);
                markerTime = merged.openDeletionTime(isReversed);
            }
        }

        public void close()
        {
            // todo: create and apply a full page mutation at once?
            // todo: separate memtable for this?
            // todo:    - allow incremental repairs (flush to repaired sstable)
            // todo:    - be able to run this with DTCS (or make flushing align with dtcs windows)
            cfs.metric.totalRows.inc();
            if (!updater.isEmpty())
            {
                metrics.appliedMutation();
                new Mutation(cfs.keyspace.getName(), partitionKey).add(updater).apply();
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
