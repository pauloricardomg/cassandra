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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
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
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowDiffListener;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.net.AsyncOneResponse;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;



public class RepairMergeListener implements UnfilteredPartitionIterators.MergeListener
{
    private static final Logger logger = LoggerFactory.getLogger(RepairMergeListener.class);

    protected final List<AsyncOneResponse> repairResults = Collections.synchronizedList(new ArrayList<>());
    protected final InetAddress[] sources;
    protected final ReadCommand command;
    private final ConsistencyLevel consistency;
    private final Keyspace keyspace;

    public RepairMergeListener(Keyspace keyspace, InetAddress[] sources, ReadCommand command, ConsistencyLevel consistency)
    {
        this.keyspace = keyspace;
        this.sources = sources;
        this.command = command;
        this.consistency = consistency;
    }

    public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
    {
        return new MergeListener(partitionKey, columns(versions), isReversed(versions), sources, command, repairResults);
    }

    protected PartitionColumns columns(List<UnfilteredRowIterator> versions)
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

    protected boolean isReversed(List<UnfilteredRowIterator> versions)
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

    public void close()
    {
        try
        {
            FBUtilities.waitOnFutures(repairResults, DatabaseDescriptor.getWriteRpcTimeout());
        }
        catch (TimeoutException ex)
        {
            // We got all responses, but timed out while repairing
            int blockFor = consistency.blockFor(keyspace);
            if (Tracing.isTracing())
                Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
            else
                logger.debug("Timeout while read-repairing after receiving all {} data and digest responses", blockFor);

            throw new ReadTimeoutException(consistency, blockFor - 1, blockFor, true);
        }
    }

    public static class MergeListener implements UnfilteredRowIterators.MergeListener
    {
        private final DecoratedKey partitionKey;
        private final PartitionColumns columns;
        private final boolean isReversed;
        protected final PartitionUpdate[] repairs;

        private final Row.Builder[] currentRows;
        private final RowDiffListener diffListener;

        private final ClusteringBound[] markerOpen;
        private final DeletionTime[] markerTime;
        protected final InetAddress[] sources;
        private final ReadCommand command;
        protected final List<AsyncOneResponse> repairResults;

        public MergeListener(DecoratedKey partitionKey, PartitionColumns columns, boolean isReversed, InetAddress[] sources, ReadCommand command, List<AsyncOneResponse> repairResults)
        {
            this.partitionKey = partitionKey;
            this.columns = columns;
            this.isReversed = isReversed;
            this.sources = sources;
            repairs = new PartitionUpdate[sources.length];
            currentRows = new Row.Builder[sources.length];
            markerOpen = new ClusteringBound[sources.length];
            markerTime = new DeletionTime[sources.length];

            this.command = command;
            this.repairResults = repairResults;

            this.diffListener = new RowDiffListener()
            {
                public void onPrimaryKeyLivenessInfo(int i, Clustering clustering, LivenessInfo merged, LivenessInfo original)
                {
                    if (merged != null && !merged.equals(original))
                        currentRow(i, clustering).addPrimaryKeyLivenessInfo(merged);
                }

                public void onDeletion(int i, Clustering clustering, Row.Deletion merged, Row.Deletion original)
                {
                    if (merged != null && !merged.equals(original))
                        currentRow(i, clustering).addRowDeletion(merged);
                }

                public void onComplexDeletion(int i, Clustering clustering, ColumnDefinition column, DeletionTime merged, DeletionTime original)
                {
                    if (merged != null && !merged.equals(original))
                        currentRow(i, clustering).addComplexDeletion(column, merged);
                }

                public void onCell(int i, Clustering clustering, Cell merged, Cell original)
                {
                    if (merged != null && !merged.equals(original) && isQueried(merged))
                        currentRow(i, clustering).addCell(merged);
                }

                private boolean isQueried(Cell cell)
                {
                    // When we read, we may have some cell that have been fetched but are not selected by the user. Those cells may
                    // have empty values as optimization (see CASSANDRA-10655) and hence they should not be included in the read-repair.
                    // This is fine since those columns are not actually requested by the user and are only present for the sake of CQL
                    // semantic (making sure we can always distinguish between a row that doesn't exist from one that do exist but has
                    /// no value for the column requested by the user) and so it won't be unexpected by the user that those columns are
                    // not repaired.
                    ColumnDefinition column = cell.column();
                    ColumnFilter filter = command.columnFilter();
                    return column.isComplex() ? filter.fetchedCellIsQueried(column, cell.path()) : filter.fetchedColumnIsQueried(column);
                }
            };
        }

        private PartitionUpdate update(int i)
        {
            if (repairs[i] == null)
                repairs[i] = new PartitionUpdate(command.metadata(), partitionKey, columns, 1);
            return repairs[i];
        }

        private Row.Builder currentRow(int i, Clustering clustering)
        {
            if (currentRows[i] == null)
            {
                currentRows[i] = BTreeRow.sortedBuilder();
                currentRows[i].newRow(clustering);
            }
            return currentRows[i];
        }

        public void onMergedPartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions)
        {
            for (int i = 0; i < versions.length; i++)
            {
                if (mergedDeletion.supersedes(versions[i]))
                    update(i).addPartitionDeletion(mergedDeletion);
            }
        }

        public void onMergedRows(Row merged, Row[] versions)
        {
            // If a row was shadowed post merged, it must be by a partition level or range tombstone, and we handle
            // those case directly in their respective methods (in other words, it would be inefficient to send a row
            // deletion as repair when we know we've already send a partition level or range tombstone that covers it).
            if (merged.isEmpty())
                return;

            Rows.diff(diffListener, merged, versions);
            for (int i = 0; i < currentRows.length; i++)
            {
                if (currentRows[i] != null)
                    update(i).add(currentRows[i].build());
            }
            Arrays.fill(currentRows, null);
        }

        public void onMergedRangeTombstoneMarkers(RangeTombstoneMarker merged, RangeTombstoneMarker[] versions)
        {
            for (int i = 0; i < versions.length; i++)
            {
                RangeTombstoneMarker marker = versions[i];
                // Note that boundaries are both close and open, so it's not one or the other
                if (merged.isClose(isReversed) && markerOpen[i] != null)
                {
                    ClusteringBound open = markerOpen[i];
                    ClusteringBound close = merged.closeBound(isReversed);
                    update(i).add(new RangeTombstone(Slice.make(isReversed ? close : open, isReversed ? open : close), markerTime[i]));
                }
                if (merged.isOpen(isReversed) && (marker == null || merged.openDeletionTime(isReversed).supersedes(marker.openDeletionTime(isReversed))))
                {
                    markerOpen[i] = merged.openBound(isReversed);
                    markerTime[i] = merged.openDeletionTime(isReversed);
                }
            }
        }

        public void close()
        {
            for (int i = 0; i < repairs.length; i++)
            {
                if (repairs[i] == null)
                    continue;

                // use a separate verb here because we don't want these to be get the white glove hint-
                // on-timeout behavior that a "real" mutation gets
                Tracing.trace("Sending read-repair-mutation to {}", sources[i]);
                MessageOut<Mutation> msg = new Mutation(repairs[i]).createMessage(MessagingService.Verb.READ_REPAIR);
                repairResults.add(MessagingService.instance().sendRR(msg, sources[i]));
            }
        }
    }
}
