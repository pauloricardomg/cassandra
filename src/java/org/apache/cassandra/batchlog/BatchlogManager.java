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
package org.apache.cassandra.batchlog;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

import static com.google.common.collect.Iterables.transform;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternalWithPaging;

public class BatchlogManager implements BatchlogManagerMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=BatchlogManager";
    private static final long REPLAY_INTERVAL = 10 * 1000; // milliseconds
    static final int DEFAULT_PAGE_SIZE = 128;

    private static final Logger logger = LoggerFactory.getLogger(BatchlogManager.class);
    public static final BatchlogManager instance = new BatchlogManager();
    public static final int MAX_CACHED_MUTATIONS_IN_BYTES = 4 * 1024 * 1024;

    private volatile long totalBatchesReplayed = 0; // no concurrency protection necessary as only written by replay thread.
    private volatile UUID lastReplayedUuid = UUIDGen.minTimeUUID(0);

    // Single-thread executor service for scheduling and serializing log replay.
    private final ScheduledExecutorService batchlogTasks;

    public BatchlogManager()
    {
        ScheduledThreadPoolExecutor executor = new DebuggableScheduledThreadPoolExecutor("BatchlogTasks");
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        batchlogTasks = executor;
    }

    public void start()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        batchlogTasks.scheduleWithFixedDelay(this::replayFailedBatches,
                                             StorageService.RING_DELAY,
                                             REPLAY_INTERVAL,
                                             TimeUnit.MILLISECONDS);
    }

    public void shutdown() throws InterruptedException
    {
        batchlogTasks.shutdown();
        batchlogTasks.awaitTermination(60, TimeUnit.SECONDS);
    }

    public static void remove(UUID id)
    {
        new Mutation(PartitionUpdate.fullPartitionDelete(SystemKeyspace.Batchlog,
                                                         UUIDType.instance.decompose(id),
                                                         FBUtilities.timestampMicros(),
                                                         FBUtilities.nowInSeconds()))
            .apply();
    }

    public static void store(Batch batch)
    {
        store(batch, true);
    }

    public static void store(Batch batch, boolean durableWrites)
    {
        int mutationCount = 0;

        for (ByteBuffer mutation : batch.encodedMutations)
        {
            writeBatchlogMutation(batch.id, batch.creationTime, mutationCount++, mutation);
        }

        for (Mutation mutation : batch.decodedMutations)
        {
            try (DataOutputBuffer buffer = new DataOutputBuffer())
            {
                Mutation.serializer.serialize(mutation, buffer, MessagingService.current_version);
                writeBatchlogMutation(batch.id, batch.creationTime, mutationCount++, buffer.buffer());
            }
            catch (IOException e)
            {
                // shouldn't happen
                throw new AssertionError(e);
            }
        }

        new RowUpdateBuilder(SystemKeyspace.Batchlog, batch.creationTime, batch.id)
            .add("version", MessagingService.current_version)
            .add("mutation_count", mutationCount)
            .build()
            .apply(durableWrites);
    }

    private static void writeBatchlogMutation(UUID batchId, long creationTime, int mutationId, ByteBuffer mutation)
    {
        new RowUpdateBuilder(SystemKeyspace.Batchlog, creationTime, batchId)
            .clustering(mutationId)
            .add("mutation", mutation)
            .build().apply(true);
    }

    @VisibleForTesting
    public int countAllBatches()
    {
        String query = String.format("SELECT DISTINCT batch_id FROM %s.%s", SystemKeyspace.NAME, SystemKeyspace.BATCHLOG);
        UntypedResultSet results = executeInternal(query);
        if (results == null || results.isEmpty())
            return 0;

        return results.size();
    }

    public long getTotalBatchesReplayed()
    {
        return totalBatchesReplayed;
    }

    public void forceBatchlogReplay() throws Exception
    {
        startBatchlogReplay().get();
    }

    public Future<?> startBatchlogReplay()
    {
        // If a replay is already in progress this request will be executed after it completes.
        return batchlogTasks.submit(this::replayFailedBatches);
    }

    void performInitialReplay() throws InterruptedException, ExecutionException
    {
        // Invokes initial replay. Used for testing only.
        batchlogTasks.submit(this::replayFailedBatches).get();
    }

    private void replayFailedBatches()
    {
        logger.trace("Started replayFailedBatches");

        // rate limit is in bytes per second. Uses Double.MAX_VALUE if disabled (set to 0 in cassandra.yaml).
        // max rate is scaled by the number of nodes in the cluster (same as for HHOM - see CASSANDRA-5272).
        int endpointsCount = StorageService.instance.getTokenMetadata().getAllEndpoints().size();
        if (endpointsCount <= 0)
        {
            logger.trace("Replay cancelled as there are no peers in the ring.");
            return;
        }
        int throttleInKB = DatabaseDescriptor.getBatchlogReplayThrottleInKB() / endpointsCount;
        RateLimiter rateLimiter = RateLimiter.create(throttleInKB == 0 ? Double.MAX_VALUE : throttleInKB * 1024);

        UUID limitUuid = UUIDGen.maxTimeUUID(System.currentTimeMillis() - getBatchlogTimeout());
        ColumnFamilyStore store = Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.BATCHLOG);
        int pageSize = calculatePageSize(store);
        // There cannot be any live content where token(id) <= token(lastReplayedUuid) as every processed batch is
        // deleted, but the tombstoned content may still be present in the tables. To avoid walking over it we specify
        // token(id) > token(lastReplayedUuid) as part of the query.
        String query = String.format("SELECT batch_id, mutation_id, mutation, mutation_count, version FROM %s.%s " +
                                     "WHERE token(batch_id) > token(?) AND token(batch_id) <= token(?)",
                                     SystemKeyspace.NAME,
                                     SystemKeyspace.BATCHLOG);
        UntypedResultSet batches = executeInternalWithPaging(query, pageSize, lastReplayedUuid, limitUuid);
        Optional<UUID> firstIncompleteBatchId = processBatchlogEntries(batches, pageSize, rateLimiter);
        lastReplayedUuid = firstIncompleteBatchId.orElse(limitUuid);
        logger.trace("Finished replayFailedBatches");
    }

    // read less rows (batches) per page if they are very large
    static int calculatePageSize(ColumnFamilyStore store)
    {
        double averageRowSize = store.getMeanPartitionSize();
        if (averageRowSize <= 0)
            return DEFAULT_PAGE_SIZE;

        return (int) Math.max(1, Math.min(DEFAULT_PAGE_SIZE, MAX_CACHED_MUTATIONS_IN_BYTES / averageRowSize));
    }

    private Optional<UUID> processBatchlogEntries(UntypedResultSet batches, int pageSize, RateLimiter rateLimiter)
    {
        UUID firstIncompleteBatchId = null;
        ArrayList<ReplayingBatch> unfinishedBatches = new ArrayList<>(pageSize);

        Set<InetAddress> hintedNodes = new HashSet<>();
        Set<UUID> replayedBatches = new HashSet<>();

        int mutationBytesInMemory = 0;
        Iterator<UntypedResultSet.Row> batchesIt = batches.iterator();
        while (batchesIt.hasNext())
        {
            Pair<UUID, ReplayingBatch> pair = getFirstIncompleteBatchIdAndNextCompleteBatch(batchesIt);
            if (pair.left != null && firstIncompleteBatchId == null)
            {
                firstIncompleteBatchId = pair.left;
            }
            ReplayingBatch nextBatch = pair.right;

            if (nextBatch != null)
            {
                if (nextBatch.replay(rateLimiter, hintedNodes) > 0)
                {
                    unfinishedBatches.add(nextBatch);
                    mutationBytesInMemory += nextBatch.getReplayedBytes();
                }
                else
                {
                    remove(nextBatch.getId()); // no write mutations were sent (either expired or all CFs involved truncated).
                    ++totalBatchesReplayed;
                }

                if (mutationBytesInMemory >= MAX_CACHED_MUTATIONS_IN_BYTES)
                {
                    // We have reached the end of a batch. To avoid keeping more than MAX_CACHED_MUTATIONS_IN_BYTES in memory,
                    // finish processing the page before requesting the next row.
                    finishAndClearBatches(unfinishedBatches, hintedNodes, replayedBatches);
                    mutationBytesInMemory = 0;
                }
            }
        }

        finishAndClearBatches(unfinishedBatches, hintedNodes, replayedBatches);

        // to preserve batch guarantees, we must ensure that hints (if any) have made it to disk, before deleting the batches
        HintsService.instance.flushAndFsyncBlockingly(transform(hintedNodes, StorageService.instance::getHostIdForEndpoint));

        // once all generated hints are fsynced, actually delete the batches
        replayedBatches.forEach(BatchlogManager::remove);
        return Optional.ofNullable(firstIncompleteBatchId);
    }


    /**
     * @return Pair<UUID, ReplayingBatch> with the first incomplete batch id, if found, and the next complete batch
     */
    private Pair<UUID, ReplayingBatch> getFirstIncompleteBatchIdAndNextCompleteBatch(Iterator<UntypedResultSet.Row> batchesIt)
    {
        UntypedResultSet.Row currentRow = batchesIt.next();
        UUID batchId = currentRow.getUUID("batch_id");
        int version = currentRow.getInt("version");
        Integer mutationCount = currentRow.getInteger("mutation_count");

        UUID firstIncompleteBatchId = null;

        if (mutationCount == null)
        {
            firstIncompleteBatchId = batchId;
            UntypedResultSet.Row nextRow = skipIncompleteBatchAndFetchNextRow(batchesIt, batchId);

            if (nextRow != null)
            {
                currentRow = nextRow;
                batchId = nextRow.getUUID("batch_id");
                version = nextRow.getInt("version");
                mutationCount = nextRow.getInteger("mutation_count");
            }
        }

        ReplayingBatch batch = null;

        if (mutationCount != null)
        {
            assert mutationCount > 0;
            batch = new ReplayingBatch(batchId, version, mutationCount);
            try
            {
                batch.addMutation(currentRow.getBlob("mutation"));
                int addedMutations = 1;
                while (batchesIt.hasNext() && addedMutations < mutationCount)
                {
                    currentRow = batchesIt.next();
                    batch.addMutation(currentRow.getBlob("mutation"));
                    addedMutations++;
                }
            }
            catch (IOException e)
            {
                logger.warn("Skipped batch replay of {} due to {}", batch.getId(), e);
                remove(batch.getId());
                batch = null;
            }
        }

        return Pair.create(firstIncompleteBatchId, batch);
    }

    private static UntypedResultSet.Row skipIncompleteBatchAndFetchNextRow(Iterator<UntypedResultSet.Row> batchesIt, UUID batchToSkip)
    {
        UntypedResultSet.Row row = null;
        UUID newBatchId = null;
        if (batchesIt.hasNext())
        {
            row = batchesIt.next();
            newBatchId = row.getUUID("batch_id");

            while (newBatchId.equals(batchToSkip) && batchesIt.hasNext())
            {
                row = batchesIt.next();
                newBatchId = row.getUUID("batch_id");
            }
        }

        if (newBatchId != null && !newBatchId.equals(batchToSkip))
            return row;
        return null;
    }

//    private void processBatchlogEntriesOld(UntypedResultSet batches, int pageSize, RateLimiter rateLimiter)
//    {
//        int positionInPage = 0;
//        ArrayList<ReplayingBatch> unfinishedBatches = new ArrayList<>(pageSize);
//
//        Set<InetAddress> hintedNodes = new HashSet<>();
//        Set<UUID> replayedBatches = new HashSet<>();
//
//        // Sending out batches for replay without waiting for them, so that one stuck batch doesn't affect others
//        for (UntypedResultSet.Row row : batches)
//        {
//            UUID id = row.getUUID("id");
//            int version = row.getInt("version");
//            try
//            {
//                ReplayingBatch batch = new ReplayingBatch(id, version, row.getList("mutations", BytesType.instance));
//                if (batch.replay(rateLimiter, hintedNodes) > 0)
//                {
//                    unfinishedBatches.add(batch);
//                }
//                else
//                {
//                    remove(id); // no write mutations were sent (either expired or all CFs involved truncated).
//                    ++totalBatchesReplayed;
//                }
//            }
//            catch (IOException e)
//            {
//                logger.warn("Skipped batch replay of {} due to {}", id, e);
//                remove(id);
//            }
//
//            if (++positionInPage == pageSize)
//            {
//                // We have reached the end of a batch. To avoid keeping more than a page of mutations in memory,
//                // finish processing the page before requesting the next row.
//                finishAndClearBatches(unfinishedBatches, hintedNodes, replayedBatches);
//                positionInPage = 0;
//            }
//        }
//
//        finishAndClearBatches(unfinishedBatches, hintedNodes, replayedBatches);
//
//        // to preserve batch guarantees, we must ensure that hints (if any) have made it to disk, before deleting the batches
//        HintsService.instance.flushAndFsyncBlockingly(transform(hintedNodes, StorageService.instance::getHostIdForEndpoint));
//
//        // once all generated hints are fsynced, actually delete the batches
//        replayedBatches.forEach(BatchlogManager::remove);
//    }

    private void finishAndClearBatches(ArrayList<ReplayingBatch> batches, Set<InetAddress> hintedNodes, Set<UUID> replayedBatches)
    {
        // schedule hints for timed out deliveries
        for (ReplayingBatch batch : batches)
        {
            batch.finish(hintedNodes);
            replayedBatches.add(batch.id);
        }

        totalBatchesReplayed += batches.size();
        batches.clear();
    }

    public static long getBatchlogTimeout()
    {
        return DatabaseDescriptor.getWriteRpcTimeout() * 2; // enough time for the actual write + BM removal mutation
    }

    private static class ReplayingBatch
    {
        private final UUID id;
        private final long writtenAt;
        private final List<Mutation> mutations;
        private int replayedBytes = 0;
        private final int version;

        private List<ReplayWriteResponseHandler<Mutation>> replayHandlers;

        ReplayingBatch(UUID id, int version, int mutationCount)
        {
            this.id = id;
            this.writtenAt = UUIDGen.unixTimestamp(id);
            this.replayHandlers = new ArrayList<>(mutationCount);
            this.mutations = new ArrayList<>(mutationCount);
            this.replayedBytes = 0;
            this.version = version;
        }

        public int getReplayedBytes()
        {
            return this.replayedBytes;
        }

        public int replay(RateLimiter rateLimiter, Set<InetAddress> hintedNodes)
        {
            logger.trace("Replaying batch {}", id);

            if (mutations.isEmpty())
                return 0;

            int gcgs = gcgs(mutations);
            if (TimeUnit.MILLISECONDS.toSeconds(writtenAt) + gcgs <= FBUtilities.nowInSeconds())
                return 0;

            replayHandlers = sendReplays(mutations, writtenAt, hintedNodes);

            rateLimiter.acquire(replayedBytes); // acquire afterwards, to not mess up ttl calculation.

            return replayHandlers.size();
        }

        public void finish(Set<InetAddress> hintedNodes)
        {
            for (int i = 0; i < replayHandlers.size(); i++)
            {
                ReplayWriteResponseHandler<Mutation> handler = replayHandlers.get(i);
                try
                {
                    handler.get();
                }
                catch (WriteTimeoutException|WriteFailureException e)
                {
                    logger.trace("Failed replaying a batched mutation to a node, will write a hint");
                    logger.trace("Failure was : {}", e.getMessage());
                    // writing hints for the rest to hints, starting from i
                    writeHintsForUndeliveredEndpoints(i, hintedNodes);
                    return;
                }
            }
        }

        // Remove CFs that have been truncated since. writtenAt and SystemTable#getTruncatedAt() both return millis.
        // We don't abort the replay entirely b/c this can be considered a success (truncated is same as delivered then
        // truncated.
        private void addMutation(ByteBuffer serializedMutation) throws IOException
        {
                replayedBytes += serializedMutation.remaining();
                try (DataInputBuffer in = new DataInputBuffer(serializedMutation, true))
                {
                    Mutation mutation = Mutation.serializer.deserialize(in, version);

                    for (UUID cfId : mutation.getColumnFamilyIds())
                        if (writtenAt <= SystemKeyspace.getTruncatedAt(cfId))
                            mutation = mutation.without(cfId);

                    if (!mutation.isEmpty())
                        mutations.add(mutation);
                }
        }

        private void writeHintsForUndeliveredEndpoints(int startFrom, Set<InetAddress> hintedNodes)
        {
            int gcgs = gcgs(mutations);

            // expired
            if (TimeUnit.MILLISECONDS.toSeconds(writtenAt) + gcgs <= FBUtilities.nowInSeconds())
                return;

            for (int i = startFrom; i < replayHandlers.size(); i++)
            {
                ReplayWriteResponseHandler<Mutation> handler = replayHandlers.get(i);
                Mutation undeliveredMutation = mutations.get(i);

                if (handler != null)
                {
                    hintedNodes.addAll(handler.undelivered);
                    HintsService.instance.write(transform(handler.undelivered, StorageService.instance::getHostIdForEndpoint),
                                                Hint.create(undeliveredMutation, writtenAt));
                }
            }
        }

        private static List<ReplayWriteResponseHandler<Mutation>> sendReplays(List<Mutation> mutations,
                                                                              long writtenAt,
                                                                              Set<InetAddress> hintedNodes)
        {
            List<ReplayWriteResponseHandler<Mutation>> handlers = new ArrayList<>(mutations.size());
            for (Mutation mutation : mutations)
            {
                ReplayWriteResponseHandler<Mutation> handler = sendSingleReplayMutation(mutation, writtenAt, hintedNodes);
                if (handler != null)
                    handlers.add(handler);
            }
            return handlers;
        }

        /**
         * We try to deliver the mutations to the replicas ourselves if they are alive and only resort to writing hints
         * when a replica is down or a write request times out.
         *
         * @return direct delivery handler to wait on or null, if no live nodes found
         */
        private static ReplayWriteResponseHandler<Mutation> sendSingleReplayMutation(final Mutation mutation,
                                                                                     long writtenAt,
                                                                                     Set<InetAddress> hintedNodes)
        {
            Set<InetAddress> liveEndpoints = new HashSet<>();
            String ks = mutation.getKeyspaceName();
            Token tk = mutation.key().getToken();

            for (InetAddress endpoint : StorageService.instance.getNaturalAndPendingEndpoints(ks, tk))
            {
                if (endpoint.equals(FBUtilities.getBroadcastAddress()))
                {
                    mutation.apply();
                }
                else if (FailureDetector.instance.isAlive(endpoint))
                {
                    liveEndpoints.add(endpoint); // will try delivering directly instead of writing a hint.
                }
                else
                {
                    hintedNodes.add(endpoint);
                    HintsService.instance.write(StorageService.instance.getHostIdForEndpoint(endpoint),
                                                Hint.create(mutation, writtenAt));
                }
            }

            if (liveEndpoints.isEmpty())
                return null;

            ReplayWriteResponseHandler<Mutation> handler = new ReplayWriteResponseHandler<>(liveEndpoints);
            MessageOut<Mutation> message = mutation.createMessage();
            for (InetAddress endpoint : liveEndpoints)
                MessagingService.instance().sendRR(message, endpoint, handler, false);
            return handler;
        }

        private static int gcgs(Collection<Mutation> mutations)
        {
            int gcgs = Integer.MAX_VALUE;
            for (Mutation mutation : mutations)
                gcgs = Math.min(gcgs, mutation.smallestGCGS());
            return gcgs;
        }

        public UUID getId()
        {
            return id;
        }

        /**
         * A wrapper of WriteResponseHandler that stores the addresses of the endpoints from
         * which we did not receive a successful reply.
         */
        private static class ReplayWriteResponseHandler<T> extends WriteResponseHandler<T>
        {
            private final Set<InetAddress> undelivered = Collections.newSetFromMap(new ConcurrentHashMap<>());

            ReplayWriteResponseHandler(Collection<InetAddress> writeEndpoints)
            {
                super(writeEndpoints, Collections.<InetAddress>emptySet(), null, null, null, WriteType.UNLOGGED_BATCH);
                undelivered.addAll(writeEndpoints);
            }

            @Override
            protected int totalBlockFor()
            {
                return this.naturalEndpoints.size();
            }

            @Override
            public void response(MessageIn<T> m)
            {
                boolean removed = undelivered.remove(m == null ? FBUtilities.getBroadcastAddress() : m.from);
                assert removed;
                super.response(m);
            }
        }
    }

    public static class EndpointFilter
    {
        private final String localRack;
        private final Multimap<String, InetAddress> endpoints;

        public EndpointFilter(String localRack, Multimap<String, InetAddress> endpoints)
        {
            this.localRack = localRack;
            this.endpoints = endpoints;
        }

        /**
         * @return list of candidates for batchlog hosting. If possible these will be two nodes from different racks.
         */
        public Collection<InetAddress> filter()
        {
            // special case for single-node data centers
            if (endpoints.values().size() == 1)
                return endpoints.values();

            // strip out dead endpoints and localhost
            ListMultimap<String, InetAddress> validated = ArrayListMultimap.create();
            for (Map.Entry<String, InetAddress> entry : endpoints.entries())
                if (isValid(entry.getValue()))
                    validated.put(entry.getKey(), entry.getValue());

            if (validated.size() <= 2)
                return validated.values();

            if (validated.size() - validated.get(localRack).size() >= 2)
            {
                // we have enough endpoints in other racks
                validated.removeAll(localRack);
            }

            if (validated.keySet().size() == 1)
            {
                // we have only 1 `other` rack
                Collection<InetAddress> otherRack = Iterables.getOnlyElement(validated.asMap().values());
                return Lists.newArrayList(Iterables.limit(otherRack, 2));
            }

            // randomize which racks we pick from if more than 2 remaining
            Collection<String> racks;
            if (validated.keySet().size() == 2)
            {
                racks = validated.keySet();
            }
            else
            {
                racks = Lists.newArrayList(validated.keySet());
                Collections.shuffle((List<String>) racks);
            }

            // grab a random member of up to two racks
            List<InetAddress> result = new ArrayList<>(2);
            for (String rack : Iterables.limit(racks, 2))
            {
                List<InetAddress> rackMembers = validated.get(rack);
                result.add(rackMembers.get(getRandomInt(rackMembers.size())));
            }

            return result;
        }

        @VisibleForTesting
        protected boolean isValid(InetAddress input)
        {
            return !input.equals(FBUtilities.getBroadcastAddress()) && FailureDetector.instance.isAlive(input);
        }

        @VisibleForTesting
        protected int getRandomInt(int bound)
        {
            return ThreadLocalRandom.current().nextInt(bound);
        }
    }
}
