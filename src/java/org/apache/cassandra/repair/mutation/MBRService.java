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
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * 1. Read up a page of our local data, figure out where the returned page starts and ends
 * 2. Hash the page and send hash + page start & end to remote nodes
 *    - first page sent starts at the beginning of the range (and with Slice.Bound.BOTTOM)
 *    - last page sent ends at the end of the range (and Slice.Bound.TOP)
 * 3. Remote nodes start reading from the start sent in #2 above
 *    - it hashes rows until the end token/clustering found in #1 OR until 2x WINDOW SIZE rows have been found
 * 4. If remote node reads at least 2x WINDOW SIZE rows, it is considered a 'HUGE' result and we will
 *    page back all the data from the remote nodes.
 * 5. If the hashes mismatch but we read a reasonable amount of rows we reply with the data and initiator
 *     diffs its local data with the received data and puts the difference in the memtable
 * 6. If the hashes match, continue
 * 7. Goto #1.
 */
public class MBRService
{
    private static final Logger logger = LoggerFactory.getLogger(MBRService.class);
    private static final ExecutorService executor = new JMXEnabledThreadPoolExecutor(1,
                                                                                     StageManager.KEEPALIVE,
                                                                                     TimeUnit.SECONDS,
                                                                                     new LinkedBlockingQueue<>(),
                                                                                     new NamedThreadFactory("MutationRepair"),
                                                                                     "internal");
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public Future<?> start(ColumnFamilyStore cfs, int windowSize, int rowsPerSecondToRepair)
    {
        stopped.set(false);
        return executor.submit(new MutationBasedRepairRunner(cfs, windowSize, rowsPerSecondToRepair, stopped));
    }

    public void stop()
    {
        stopped.set(true);
    }

    public static class MutationBasedRepairRunner implements Runnable
    {
        private final ColumnFamilyStore cfs;
        private final int windowSize;
        private final int rowsPerSecondToRepair;
        private final AtomicBoolean stopped;

        public MutationBasedRepairRunner(ColumnFamilyStore cfs, int windowSize, int rowsPerSecondToRepair, AtomicBoolean stopped)
        {
            this.cfs = cfs;
            this.windowSize = windowSize;
            this.rowsPerSecondToRepair = rowsPerSecondToRepair;
            this.stopped = stopped;
        }

        public void run()
        {
            RateLimiter limiter = RateLimiter.create(rowsPerSecondToRepair);
            for (Range<Token> r : getRangesWithSavedStartPoint(cfs))
            {
                MBRMetricHolder metrics = new MBRMetricHolder(cfs, r);
                logger.debug("repairing range {}, windowSize={}, rowsPerSecond={}", r, windowSize, rowsPerSecondToRepair);
                DataRange dr = new DataRange(Range.makeRowRange(r), new ClusteringIndexSliceFilter(Slices.ALL, false));
                int nowInSeconds = FBUtilities.nowInSeconds();
                DecoratedKey lastInPage = null;
                PartitionRangeReadCommand rc = new PartitionRangeReadCommand(cfs.metadata,
                                                                             nowInSeconds,
                                                                             ColumnFilter.all(cfs.metadata),
                                                                             RowFilter.NONE,
                                                                             DataLimits.NONE,
                                                                             dr,
                                                                             Optional.empty());
                QueryPager pager = rc.getPager(null, Server.CURRENT_VERSION);
                PartitionPosition start = r.left.maxKeyBound();
                ByteBuffer clusteringFrom = ByteBufferUtil.EMPTY_BYTE_BUFFER;

                while (!pager.isExhausted())
                {
                    if (stopped.get())
                        return;
                    byte[] hash;
                    int count;
                    UnfilteredPartitionIterators.DigestInfo newDigest = null;
                    try (ReadExecutionController executionController = rc.executionController();
                         UnfilteredPartitionIterator pi = pager.fetchUnfilteredPageInternal(windowSize, cfs.metadata, executionController))
                    {
                        DataLimits.Counter c = rc.limits().newCounter(nowInSeconds, true).onlyCount();
                        Pair<UnfilteredPartitionIterators.DigestInfo, byte[]> digestInfo = digest(rc, c.applyTo(pi));
                        count = c.counted();
                        hash = digestInfo.right;
                        newDigest = digestInfo.left;
                    }

                    metrics.increaseRowsHashed(count);
                    PagingState ps = pager.state();
                    PartitionPosition readUntil = ps == null ? r.right.maxKeyBound() : PartitionPosition.ForKey.get(ps.partitionKey, cfs.getPartitioner());
                    // if the pager is exhausted we need to read until the end of the range on the remote node to make sure
                    // we don't miss any tokens between our last token and the end of the range
                    PartitionPosition pageEnd = pager.isExhausted() ? r.right.maxKeyBound() : readUntil;
                    // same idea as for tokens above - if pager is exhausted (or the pager didn't return any rows at all)
                    // we need to read all the (remaining) data on the remote node
                    ByteBuffer pageClusteringEnd = (pager.isExhausted() || ps == null || ps.rowMark == null) ? ByteBufferUtil.EMPTY_BYTE_BUFFER : ps.rowMark.mark;
                    boolean isStartKeyInclusive = (lastInPage == null || lastInPage.equals(newDigest.first));
                    lastInPage = newDigest.last;
                    MBRRepairPage rp = new MBRRepairPage(start, pageEnd, clusteringFrom, pageClusteringEnd, hash, count, windowSize, isStartKeyInclusive);
                    if (logger.isTraceEnabled())
                        logger.trace("Repairing page = {}", rp.toString(cfs.metadata));
                    Set<InetAddress> targets = StorageService.instance.getLiveNaturalEndpoints(cfs.keyspace, r.right).stream().filter(i -> !FBUtilities.getBroadcastAddress().equals(i)).collect(Collectors.toSet());
                    CountDownLatch cdl = new CountDownLatch(targets.size());
                    MBRResponseCallback callback = new MBRResponseCallback(cfs, rp, nowInSeconds, cdl, targets.size(), metrics);
                    for (InetAddress address : targets) // since we are repairing one local range at a time, all keys in that range will have the same replicas
                        MessagingService.instance().sendRR(new MBRCommand(cfs.metadata.cfId, nowInSeconds, rp).createMessage(), address, callback);

                    try
                    {
                        cdl.await(1, TimeUnit.MINUTES);
                    }
                    catch (InterruptedException e)
                    {
                        String message = String.format("Missing reply from %s - repair failed", Sets.difference(targets, callback.replies()));
                        logger.error(message);
                        throw new RuntimeException(message);
                    }
                    start = readUntil;
                    clusteringFrom = (ps == null || ps.rowMark == null) ? ByteBufferUtil.EMPTY_BYTE_BUFFER : ps.rowMark.mark;
                    if (count > 0)
                        limiter.acquire(count);
                }
                try
                {
                    SystemKeyspace.storeRepairedRange(cfs.keyspace.getName(), cfs.getTableName(), r, nowInSeconds);
                }
                catch (Throwable t)
                {
                    logger.warn("Unable to store last repaired range "+r, t);
                }
                logger.debug("Range finished: {}", metrics);
            }
        }
    }

    /**
     * Figure out where we should start repairing - reads the last repaired range from a system keyspace
     * and starts at the next one.
     *
     * Still returns all local ranges, just that the first one to repair will be the next range after the one
     * that we repaired last in the last round;
     *
     * @param cfs
     * @return
     */
    private static Iterable<Range<Token>> getRangesWithSavedStartPoint(ColumnFamilyStore cfs)
    {
        Range<Token> start = SystemKeyspace.getLastRepairedRange(cfs.keyspace.getName(), cfs.getTableName(), cfs.getPartitioner());
        Collection<Range<Token>> rangesToRepair = StorageService.instance.getLocalRanges(cfs.keyspace.getName());
        while (rangesToRepair == null || rangesToRepair.isEmpty())
        {
            rangesToRepair = StorageService.instance.getLocalRanges(cfs.keyspace.getName());
            FBUtilities.sleepQuietly(1000);
            logger.info("waiting for ranges to repair");
        }
        // todo: consider if we should normalize the full ranges. thinking here is that having many small ranges gives us natural
        // todo: points to save progress (we could save at given time points or something instead)
        List<Range<Token>> ranges = Range.sort(rangesToRepair.stream().map(range -> Range.normalize(Collections.singleton(range))).flatMap(Collection::stream).collect(Collectors.toList()));
        if (start == null)
            return ranges;
        int idx = 0;
        for (Range<Token> r : ranges)
        {
            if (start.right.compareTo(r.right) < 0)
                break;
            idx++;
        }
        if (idx >= ranges.size())
            idx = 0;

        List<Range<Token>> res = new ArrayList<>(ranges.size());
        res.addAll(ranges.subList(idx, ranges.size()));
        res.addAll(ranges.subList(0, idx));
        assert res.size() == ranges.size();
        return res;
    }

    public static Pair<UnfilteredPartitionIterators.DigestInfo, byte[]> digest(ReadCommand rc, UnfilteredPartitionIterator pi)
    {
        MessageDigest digest = FBUtilities.threadLocalMD5Digest();
        UnfilteredPartitionIterators.DigestInfo di = UnfilteredPartitionIterators.digest(rc, pi, digest, rc.digestVersion());
        return Pair.create(di, digest.digest());
    }

    public static class MBRDataRange extends DataRange
    {

        private final ClusteringBound until;
        private final ClusteringBound start;
        private final ClusteringComparator comparator;

        /**
         * Creates a {@code DataRange} given a range of partition keys and a clustering index filter. The
         * return {@code DataRange} will return the same filter for all keys.
         *
         * @param range                 the range over partition keys to use.
         */
        public MBRDataRange(AbstractBounds<PartitionPosition> range, ClusteringComparator comparator, ClusteringBound start, ClusteringBound until)
        {
            super(range, new ClusteringIndexSliceFilter(Slices.ALL, false));
            this.start = start;
            this.until = until;
            this.comparator = comparator;
        }

        @Override
        public ClusteringIndexFilter clusteringIndexFilter(DecoratedKey key)
        {
            if (startKey().equals(stopKey()) && key.equals(startKey()))
            {
                Slices s = new Slices.Builder(comparator).add(Slice.make(start, until)).build();
                return new ClusteringIndexSliceFilter(s, false);
            }
            else if (startKey().equals(key))
            {
                Slices s = new Slices.Builder(comparator).add(Slice.make(start, ClusteringBound.TOP)).build();
                return new ClusteringIndexSliceFilter(s, false);
            }
            else if (stopKey().equals(key))
            {
                Slices s = new Slices.Builder(comparator).add(Slice.make(ClusteringBound.BOTTOM, until)).build();
                return new ClusteringIndexSliceFilter(s, false);
            }
            return clusteringIndexFilter;

        }
    }
}
