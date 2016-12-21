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
import java.util.UUID;
import java.util.function.BiFunction;

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
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.service.StorageProxy.asyncRemoveFromBatchlog;
import static org.apache.cassandra.service.StorageProxy.canDoLocalRequest;
import static org.apache.cassandra.service.StorageProxy.performLocally;
import static org.apache.cassandra.service.StorageProxy.sendToHintedEndpoints;

public class BatchlogWriter
{
    private static final Logger logger = LoggerFactory.getLogger(StorageProxy.class);

    public static void mutateBatch(ConsistencyLevel consistencyLevel, boolean requireQuorumForRemove, long queryStartNanoTime,
                                   Collection<Mutation> mutations)
    {
        mutateAtomically(consistencyLevel, requireQuorumForRemove, queryStartNanoTime, mutations, false, StorageService.instance::getNaturalEndpoints);
    }

    public static void mutateBaseAndViews(Collection<Mutation> mutations, BiFunction<String, RingPosition, List<InetAddress>> naturalEndpointSelector)
    {
        mutateAtomically(ConsistencyLevel.ONE, false, System.nanoTime(), mutations, true, naturalEndpointSelector);
    }

    private static void mutateAtomically(ConsistencyLevel clientCL, boolean requireQuorumForRemove, long queryStartNanoTime,
                                         Collection<Mutation> mutations, boolean isBaseOrView,  BiFunction<String, RingPosition, List<InetAddress>> naturalEndpointSelector)
    {
        String localDC = DatabaseDescriptor.getLocalDataCenter();
        ConsistencyLevel batchCL = getBatchWriteCL(clientCL, requireQuorumForRemove);
        Collection<InetAddress> batchlogEndpoints = isBaseOrView? Collections.singleton(FBUtilities.getBroadcastAddress()) : getBatchlogEndpoints(localDC, batchCL);
        List<StorageProxy.WriteResponseHandlerWrapper> wrappers = new ArrayList<>(mutations.size());

        final UUID batchUUID = UUIDGen.getTimeUUID();
        BatchlogResponseHandler.BatchlogCleanup cleanup = new BatchlogResponseHandler.BatchlogCleanup(mutations.size(), () -> asyncRemoveFromBatchlog(batchlogEndpoints, batchUUID));

        // add a handler for each mutation - includes checking availability, but doesn't initiate any writes, yet
        for (Mutation mutation : mutations)
        {
            StorageProxy.WriteResponseHandlerWrapper wrapper = wrapBatchResponseHandler(mutation,
                                                                                        clientCL,
                                                                                        batchCL,
                                                                                        isBaseOrView? WriteType.VIEW : WriteType.BATCH,
                                                                                        cleanup,
                                                                                        queryStartNanoTime,
                                                                                        naturalEndpointSelector);
            // exit early if we can't fulfill the CL at this time.
            wrapper.handler.assureSufficientLiveNodes();
            wrappers.add(wrapper);
        }

        // write to the batchlog
        WriteResponseHandler<?> handler = new WriteResponseHandler<>(batchlogEndpoints,
                                                                     Collections.emptyList(),
                                                                     batchlogEndpoints.size() == 1 ? ConsistencyLevel.ONE : ConsistencyLevel.TWO,
                                                                     Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME),
                                                                     null,
                                                                     WriteType.BATCH_LOG,
                                                                     queryStartNanoTime);

        Batch batch = Batch.createLocal(batchUUID, FBUtilities.timestampMicros(), mutations, isBaseOrView ? Batch.Type.VIEWLOG : Batch.Type.BATCHLOG);
        MessageOut<Batch> message = new MessageOut<>(MessagingService.Verb.BATCH_STORE, batch, Batch.serializer);
        for (InetAddress target : batchlogEndpoints)
        {
            logger.trace("Sending batchlog store request {} to {} for {} mutations", batch.id, target, batch.size());

            if (canDoLocalRequest(target))
                performLocally(Stage.MUTATION, Optional.empty(), () -> BatchlogManager.store(batch), handler);
            else
                MessagingService.instance().sendRR(message, target, handler);
        }

        handler.get();

        for (StorageProxy.WriteResponseHandlerWrapper wrapper : wrappers)
        {
            Iterable<InetAddress> endpoints = Iterables.concat(wrapper.handler.naturalEndpoints, wrapper.handler.pendingEndpoints);
            sendToHintedEndpoints(wrapper.mutation, endpoints, wrapper.handler, localDC, isBaseOrView? Stage.VIEW_MUTATION : Stage.MUTATION);
        }

        for (StorageProxy.WriteResponseHandlerWrapper wrapper : wrappers)
            wrapper.handler.get();
    }

    // same as performWrites except does not initiate writes (but does perform availability checks).
    private static StorageProxy.WriteResponseHandlerWrapper wrapBatchResponseHandler(Mutation mutation,
                                                                                     ConsistencyLevel consistency_level,
                                                                                     ConsistencyLevel batchConsistencyLevel,
                                                                                     WriteType writeType,
                                                                                     BatchlogResponseHandler.BatchlogCleanup cleanup,
                                                                                     long queryStartNanoTime,
                                                                                     BiFunction<String, RingPosition, List<InetAddress>> naturalEndpointSelector)
    {
        Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
        AbstractReplicationStrategy rs = keyspace.getReplicationStrategy();
        String keyspaceName = mutation.getKeyspaceName();
        Token tk = mutation.key().getToken();
        List<InetAddress> naturalEndpoints = naturalEndpointSelector.apply(keyspaceName, tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspaceName);
        AbstractWriteResponseHandler<IMutation> writeHandler = rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistency_level, null, writeType, queryStartNanoTime);
        BatchlogResponseHandler<IMutation> batchHandler = new BatchlogResponseHandler<>(writeHandler, batchConsistencyLevel.blockFor(keyspace), cleanup, queryStartNanoTime);
        return new StorageProxy.WriteResponseHandlerWrapper(batchHandler, mutation);
    }

    /**
     * If we are requiring quorum nodes for removal, we upgrade consistency level to QUORUM unless we already
     * require ALL, or EACH_QUORUM. This is so that *at least* QUORUM nodes see the update.
     */
    private static ConsistencyLevel getBatchWriteCL(ConsistencyLevel clientCL, boolean requireQuorumForRemove)
    {
        if (clientCL.equals(ConsistencyLevel.ALL) || clientCL.equals(ConsistencyLevel.EACH_QUORUM))
            return clientCL;

        return requireQuorumForRemove? ConsistencyLevel.QUORUM : clientCL;
    }

    /*
     * Replicas are picked manually:
     * - replicas should be alive according to the failure detector
     * - replicas should be in the local datacenter
     * - choose min(2, number of qualifying candiates above)
     * - allow the local node to be the only replica only if it's a single-node DC
     */
    private static Collection<InetAddress> getBatchlogEndpoints(String localDataCenter, ConsistencyLevel consistencyLevel)
    throws UnavailableException
    {
        TokenMetadata.Topology topology = StorageService.instance.getTokenMetadata().cachedOnlyTokenMap().getTopology();
        Multimap<String, InetAddress> localEndpoints = HashMultimap.create(topology.getDatacenterRacks().get(localDataCenter));
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
}
