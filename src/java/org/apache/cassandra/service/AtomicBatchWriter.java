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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.batchlog.Batch;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.UUIDSerializer;

import static org.apache.cassandra.service.StorageProxy.canDoLocalRequest;
import static org.apache.cassandra.service.StorageProxy.performLocally;

public class AtomicBatchWriter
{
    private static final Logger logger = LoggerFactory.getLogger(AtomicBatchWriter.class);

    public static void storeBatchlog(Collection<Mutation> mutations)
    {
        Batch batch = Batch.createLocal(UUIDGen.getTimeUUID(), FBUtilities.timestampMicros(), mutations, true);
        BatchlogManager.store(batch);
    }

    protected static AtomicBatchWriteHandler mutateAtomicallyAsync(ConsistencyLevel clientCL,
                                                                   boolean requireQuorumForRemove,
                                                                   long queryStartNanoTime,
                                                                   Collection<Mutation> batchMutations,
                                                                   Set<Mutation> skipWrite,
                                                                   boolean isViewBatchlog,
                                                                   BiFunction<String, RingPosition, List<InetAddress>> naturalEndpointSelector,
                                                                   Runnable writeCallback)
    {
        UUID batchId = UUIDGen.getTimeUUID();
        ConsistencyLevel batchCL = getBatchConsistencyLevel(clientCL, requireQuorumForRemove);
        Collection<InetAddress> batchEndpoints = getBatchlogEndpoints(isViewBatchlog, batchCL);
        BatchlogCleanup batchlogCleanup = new BatchlogCleanup(batchId, batchEndpoints, batchMutations.size(), isViewBatchlog);

        List<BatchlogMutationWriter> batchMutationWriters = batchMutations.stream()
                                                                     .filter(m -> !skipWrite.contains(m))
                                                                     .map(m -> createBatchlogMutationWriter(m,
                                                                                                            clientCL,
                                                                                                            batchCL,
                                                                                                            isViewBatchlog,
                                                                                                            queryStartNanoTime,
                                                                                                            naturalEndpointSelector,
                                                                                                            batchlogCleanup,
                                                                                                            writeCallback))
                                                                     .collect(Collectors.toList());

        // exit early if we can't fulfill the CL at this time.
        batchMutationWriters.forEach(w -> w.assureSufficientLiveNodes());

        Batch batch = Batch.createLocal(batchId, FBUtilities.timestampMicros(), batchMutations, isViewBatchlog);

        storeBatchSync(batch, batchEndpoints, queryStartNanoTime, isViewBatchlog);

        return writeBatchlogMutationsAsync(batchMutationWriters, batchlogCleanup);
    }

    private static AtomicBatchWriteHandler writeBatchlogMutationsAsync(List<BatchlogMutationWriter> mutationWriters,
                                                                       BatchlogCleanup cleanup)
    {
        List<BatchlogMutationWriter> localWriters = new ArrayList<>(mutationWriters.size());
        List<BatchlogMutationWriter> remoteWriters = new ArrayList<>(mutationWriters.size());
        for (BatchlogMutationWriter writer : mutationWriters)
        {
            if (writer.isLocal)
            {
                localWriters.add(writer);
            }
            else
            {
                remoteWriters.add(writer);
            }
        }
        //submit local mutations first
        localWriters.stream().forEach(w -> w.writeAsync());
        remoteWriters.stream().forEach(w -> w.writeAsync());
        return new AtomicBatchWriteHandler(cleanup, localWriters, remoteWriters);
    }

    private static ConsistencyLevel getBatchConsistencyLevel(ConsistencyLevel clientCL, boolean requireQuorumForRemove)
    {
        if (requireQuorumForRemove && clientCL != clientCL)
            return ConsistencyLevel.QUORUM;

        return clientCL;
    }

    private static void storeBatchSync(Batch batch, Collection<InetAddress> batchlogEndpoints,
                                       long queryStartNanoTime, boolean isViewBatch)
    {
        if (isViewBatch)
        {
            BatchlogManager.store(batch);
            return;
        }

        // write to the batchlog
        WriteResponseHandler<?> batchlogWriteHandler = new WriteResponseHandler<>(batchlogEndpoints,
                                                                                  Collections.emptyList(),
                                                                                  batchlogEndpoints.size() == 1 ? ConsistencyLevel.ONE : ConsistencyLevel.TWO,
                                                                                  Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME),
                                                                                  null,
                                                                                  WriteType.BATCH_LOG,
                                                                                  queryStartNanoTime);

        MessageOut<Batch> message = new MessageOut<>(MessagingService.Verb.BATCH_STORE, batch, Batch.serializer);
        for (InetAddress target : batchlogWriteHandler.naturalEndpoints)
        {
            logger.trace("Sending batchlog store request {} to {} for {} mutations", batch.id, target, batch.size());

            if (canDoLocalRequest(target))
            {
                performLocally(Stage.MUTATION, Optional.empty(), () -> BatchlogManager.store(batch), batchlogWriteHandler);
            }
            else
                MessagingService.instance().sendRR(message, target, batchlogWriteHandler);
        }

        batchlogWriteHandler.get();
    }

    // same as performWrites except does not initiate writes (but does writeAsync availability checks).
    private static BatchlogMutationWriter createBatchlogMutationWriter(Mutation mutation,
                                                                       ConsistencyLevel writeCL,
                                                                       ConsistencyLevel batchCL,
                                                                       boolean isViewBatchlog,
                                                                       long queryStartNanoTime,
                                                                       BiFunction<String, RingPosition, List<InetAddress>> naturalEndpointSelector,
                                                                       BatchlogCleanup batchlogCleanup,
                                                                       Runnable writeCallback)
    {
        String ksName = mutation.getKeyspaceName();
        DecoratedKey key = mutation.key();
        List<InetAddress> naturalEndpoints = naturalEndpointSelector.apply(ksName, key);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(key.getToken(), ksName);
        boolean isLocal = naturalEndpoints.stream().anyMatch(e -> StorageProxy.canDoLocalRequest(e));
        Runnable ackCallback = null;
        if (isViewBatchlog && !isLocal)
        {
            StorageProxy.viewWriteMetrics.viewReplicasAttempted.inc(1);
            ackCallback = () -> StorageProxy.viewWriteMetrics.viewReplicasSuccess.inc();
        }
        BatchlogResponseHandler<IMutation> batchHandler = new BatchlogResponseHandler(Keyspace.open(ksName),
                                                                                      naturalEndpoints,
                                                                                      pendingEndpoints,
                                                                                      writeCL,
                                                                                      batchCL,
                                                                                      isViewBatchlog? WriteType.VIEW : WriteType.BATCH,
                                                                                      queryStartNanoTime,
                                                                                      batchlogCleanup,
                                                                                      writeCallback,
                                                                                      ackCallback);
        return new BatchlogMutationWriter(batchHandler, mutation, isLocal);
    }

    /*
     * Replicas are picked manually:
     * - replicas should be alive according to the failure detector
     * - replicas should be in the local datacenter
     * - choose min(2, number of qualifying candiates above)
     * - allow the local node to be the only replica only if it's a single-node DC
     */
    private static Collection<InetAddress> getBatchlogEndpoints(boolean isViewBatchlog, ConsistencyLevel consistencyLevel)
    throws UnavailableException
    {
        if (isViewBatchlog)
            return Collections.singleton(FBUtilities.getBroadcastAddress());

        TokenMetadata.Topology topology = StorageService.instance.getTokenMetadata().cachedOnlyTokenMap().getTopology();
        Multimap<String, InetAddress> localEndpoints = HashMultimap.create(topology.getDatacenterRacks().get(DatabaseDescriptor.getLocalDataCenter()));
        String localRack = DatabaseDescriptor.getEndpointSnitch().getRack(FBUtilities.getBroadcastAddress());

        Collection<InetAddress> chosenEndpoints = new BatchlogManager.EndpointFilter(localRack, localEndpoints).filter();
        if (chosenEndpoints.isEmpty())
        {
            if (consistencyLevel == ConsistencyLevel.ANY)
                return Collections.singleton(FBUtilities.getBroadcastAddress());

            throw new UnavailableException(ConsistencyLevel.ONE, 1, 0);
        }

        return chosenEndpoints;
    }

    public static class AtomicBatchWriteHandler
    {
        private final List<BatchlogMutationWriter> localWriters;
        private final List<BatchlogMutationWriter> remoteWriters;
        private final BatchlogCleanup cleanup;

        public AtomicBatchWriteHandler(BatchlogCleanup cleanup,
                                       List<BatchlogMutationWriter> localWriters,
                                       List<BatchlogMutationWriter> remoteWriters)
        {
            this.localWriters = localWriters;
            this.remoteWriters = remoteWriters;
            this.cleanup = cleanup;
        }

        public void ackMutation()
        {
            cleanup.ackMutation();
        }

        public void waitForLocalWrites() throws WriteTimeoutException, WriteFailureException
        {
            localWriters.stream().forEach(w -> w.waitCompletion());
        }

        public void waitForAllWrites()
        {
            localWriters.stream().forEach(w -> w.waitCompletion());
            remoteWriters.stream().forEach(w -> w.waitCompletion());
        }
    }

    // used by atomic_batch_mutate to decouple availability check from the write itself, caches consistency level and endpoints.
    static class BatchlogMutationWriter
    {
        final static String LOCAL_DC = DatabaseDescriptor.getLocalDataCenter();
        final BatchlogResponseHandler<IMutation> handler;
        final Mutation mutation;
        private final boolean isLocal;

        BatchlogMutationWriter(BatchlogResponseHandler<IMutation> handler, Mutation mutation, boolean isLocal)
        {
            this.handler = handler;
            this.mutation = mutation;
            this.isLocal = isLocal;
        }

        public void writeAsync()
        {
            Iterable<InetAddress> endpoints = Iterables.concat(handler.naturalEndpoints, handler.pendingEndpoints);
            Stage stage = handler.writeType == WriteType.VIEW ? Stage.VIEW_MUTATION : Stage.MUTATION;
            StorageProxy.sendToHintedEndpoints(mutation, endpoints, handler, LOCAL_DC, stage);
        }

        public void waitCompletion() throws WriteTimeoutException, WriteFailureException
        {
            handler.get();
        }

        public void assureSufficientLiveNodes()
        {
            handler.assureSufficientLiveNodes();
        }
    }

    public static class BatchlogCleanup
    {
        protected volatile int mutationsWaitingFor;
        private static final AtomicIntegerFieldUpdater<BatchlogCleanup> mutationsWaitingForUpdater
        = AtomicIntegerFieldUpdater.newUpdater(BatchlogCleanup.class, "mutationsWaitingFor");
        private Collection<InetAddress> endpoints;
        private UUID batchId;
        private boolean isViewBatchlog;

        public BatchlogCleanup(UUID batchId, Collection<InetAddress> endpoints, int mutationsCount, boolean isViewBatchlog)
        {
            this.batchId = batchId;
            this.endpoints = endpoints;
            this.mutationsWaitingFor = mutationsCount;
            this.isViewBatchlog = isViewBatchlog;
        }

        public void ackMutation()
        {
            if (mutationsWaitingForUpdater.decrementAndGet(this) == 0)
                asyncRemoveFromBatchlog();
        }

        protected void asyncRemoveFromBatchlog()
        {
            logger.trace("Cleaning up batchlog {}", batchId);
            MessageOut<UUID> message = new MessageOut<>(MessagingService.Verb.BATCH_REMOVE, batchId, UUIDSerializer.serializer);
            for (InetAddress target : endpoints)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Sending batchlog remove request {} to {}", batchId, target);

                if (canDoLocalRequest(target))
                    performLocally(isViewBatchlog? Stage.VIEW_MUTATION : Stage.MUTATION, () -> BatchlogManager.remove(batchId));
                else
                    MessagingService.instance().sendOneWay(message, target);
            }
        }
    }

    static class BatchlogResponseHandler<T> extends AbstractWriteResponseHandler<T>
    {
        private final AtomicBatchWriter.BatchlogCleanup cleanup;
        private final Runnable ackCallback;
        AbstractWriteResponseHandler<T> wrapped;
        protected volatile int responsesBeforeAck;
        private static final AtomicIntegerFieldUpdater<BatchlogResponseHandler> requiredBeforeFinishUpdater = AtomicIntegerFieldUpdater.newUpdater(BatchlogResponseHandler.class, "responsesBeforeAck");

        public BatchlogResponseHandler(Keyspace keyspace,
                                       Collection<InetAddress> naturalEndpoints,
                                       Collection<InetAddress> pendingEndpoints,
                                       ConsistencyLevel writeCL,
                                       ConsistencyLevel batchCL,
                                       WriteType writeType,
                                       long queryStartNanoTime,
                                       AtomicBatchWriter.BatchlogCleanup batchlogCleanup,
                                       Runnable writeCallback,
                                       Runnable ackCallback)
        {
            super(keyspace, naturalEndpoints, pendingEndpoints, writeCL, null, writeType, queryStartNanoTime);
            this.wrapped = keyspace.getReplicationStrategy().getWriteResponseHandler(naturalEndpoints, pendingEndpoints, writeCL, writeCallback, writeType, queryStartNanoTime);
            this.responsesBeforeAck = batchCL.blockFor(keyspace);
            this.cleanup = batchlogCleanup;
            this.ackCallback = ackCallback;
        }

        protected int ackCount()
        {
            return wrapped.ackCount();
        }

        public void response(MessageIn<T> msg)
        {
            if (ackCallback != null)
                ackCallback.run();
            wrapped.response(msg);
            if (requiredBeforeFinishUpdater.decrementAndGet(this) == 0)
                cleanup.ackMutation();
        }

        public boolean isLatencyForSnitch()
        {
            return wrapped.isLatencyForSnitch();
        }

        public void onFailure(InetAddress from, RequestFailureReason failureReason)
        {
            wrapped.onFailure(from, failureReason);
        }

        public void assureSufficientLiveNodes()
        {
            wrapped.assureSufficientLiveNodes();
        }

        public void get() throws WriteTimeoutException, WriteFailureException
        {
            wrapped.get();
        }

        protected int totalBlockFor()
        {
            return wrapped.totalBlockFor();
        }

        protected int totalEndpoints()
        {
            return wrapped.totalEndpoints();
        }

        protected boolean waitingFor(InetAddress from)
        {
            return wrapped.waitingFor(from);
        }

        protected void signal()
        {
            wrapped.signal();
        }
    }
}
