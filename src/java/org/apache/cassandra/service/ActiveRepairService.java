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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.AnticompactionTask;
import org.apache.cassandra.repair.NodePair;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.RepairSession;
import org.apache.cassandra.repair.StreamingRepairTask;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.repair.messages.*;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.Refs;

/**
 * ActiveRepairService is the starting point for manual "active" repairs.
 *
 * Each user triggered repair will correspond to one or multiple repair session,
 * one for each token range to repair. On repair session might repair multiple
 * column families. For each of those column families, the repair session will
 * request merkle trees for each replica of the range being repaired, diff those
 * trees upon receiving them, schedule the streaming ofthe parts to repair (based on
 * the tree diffs) and wait for all those operation. See RepairSession for more
 * details.
 *
 * The creation of a repair session is done through the submitRepairSession that
 * returns a future on the completion of that session.
 */
public class ActiveRepairService
{
    public static CassandraVersion SUPPORTS_GLOBAL_PREPARE_FLAG_VERSION = new CassandraVersion("2.2.1");

    private static final Logger logger = LoggerFactory.getLogger(ActiveRepairService.class);
    // singleton enforcement
    public static final ActiveRepairService instance = new ActiveRepairService(FailureDetector.instance, Gossiper.instance);

    public static final long UNREPAIRED_SSTABLE = 0;

    /**
     * A map of active coordinator session.
     */
    private final ConcurrentMap<UUID, RepairSession> sessions = new ConcurrentHashMap<>();

    private final ConcurrentMap<UUID, ParentRepairSession> parentRepairSessions = new ConcurrentHashMap<>();

    private final IFailureDetector failureDetector;
    private final Gossiper gossiper;

    public ActiveRepairService(IFailureDetector failureDetector, Gossiper gossiper)
    {
        this.failureDetector = failureDetector;
        this.gossiper = gossiper;
    }

    /**
     * Requests repairs for the given keyspace and column families.
     *
     * @return Future for asynchronous call or null if there is no need to repair
     */
    public RepairSession submitRepairSession(UUID parentRepairSession,
                                             Collection<Range<Token>> range,
                                             String keyspace,
                                             RepairParallelism parallelismDegree,
                                             Set<InetAddress> endpoints,
                                             long repairedAt,
                                             ListeningExecutorService executor,
                                             String... cfnames)
    {
        if (endpoints.isEmpty())
            return null;

        if (cfnames.length == 0)
            return null;

        final RepairSession session = new RepairSession(parentRepairSession, UUIDGen.getTimeUUID(), range, keyspace, parallelismDegree, endpoints, repairedAt, cfnames);
        ParentRepairSession prs = getParentRepairSession(parentRepairSession);
        prs.registerRepairSession(session);

        sessions.put(session.getId(), session);
        // register listeners
        gossiper.register(session);
        failureDetector.registerFailureDetectionEventListener(session);

        // unregister listeners at completion
        session.addListener(new Runnable()
        {
            /**
             * When repair finished, do clean up
             */
            public void run()
            {
                failureDetector.unregisterFailureDetectionEventListener(session);
                gossiper.unregister(session);
                sessions.remove(session.getId());
            }
        }, MoreExecutors.sameThreadExecutor());
        session.start(executor);
        return session;
    }

    public synchronized void terminateSessions()
    {
        Throwable cause = new IOException("Terminate session is called");
        for (RepairSession session : sessions.values())
        {
            session.forceShutdown(cause);
        }
        parentRepairSessions.clear();
    }

    /**
     * Return all of the neighbors with whom we share the provided range.
     *
     * @param keyspaceName keyspace to repair
     * @param toRepair token to repair
     * @param dataCenters the data centers to involve in the repair
     *
     * @return neighbors with whom we share the provided range
     */
    public static Set<InetAddress> getNeighbors(String keyspaceName, Range<Token> toRepair, Collection<String> dataCenters, Collection<String> hosts)
    {
        StorageService ss = StorageService.instance;
        Map<Range<Token>, List<InetAddress>> replicaSets = ss.getRangeToAddressMap(keyspaceName);
        Range<Token> rangeSuperSet = null;
        for (Range<Token> range : ss.getLocalRanges(keyspaceName))
        {
            if (range.contains(toRepair))
            {
                rangeSuperSet = range;
                break;
            }
            else if (range.intersects(toRepair))
            {
                throw new IllegalArgumentException("Requested range intersects a local range but is not fully contained in one; this would lead to imprecise repair");
            }
        }
        if (rangeSuperSet == null || !replicaSets.containsKey(rangeSuperSet))
            return Collections.emptySet();

        Set<InetAddress> neighbors = new HashSet<>(replicaSets.get(rangeSuperSet));
        neighbors.remove(FBUtilities.getBroadcastAddress());

        if (dataCenters != null && !dataCenters.isEmpty())
        {
            TokenMetadata.Topology topology = ss.getTokenMetadata().cloneOnlyTokenMap().getTopology();
            Set<InetAddress> dcEndpoints = Sets.newHashSet();
            Multimap<String,InetAddress> dcEndpointsMap = topology.getDatacenterEndpoints();
            for (String dc : dataCenters)
            {
                Collection<InetAddress> c = dcEndpointsMap.get(dc);
                if (c != null)
                   dcEndpoints.addAll(c);
            }
            return Sets.intersection(neighbors, dcEndpoints);
        }
        else if (hosts != null && !hosts.isEmpty())
        {
            Set<InetAddress> specifiedHost = new HashSet<>();
            for (final String host : hosts)
            {
                try
                {
                    final InetAddress endpoint = InetAddress.getByName(host.trim());
                    if (endpoint.equals(FBUtilities.getBroadcastAddress()) || neighbors.contains(endpoint))
                        specifiedHost.add(endpoint);
                }
                catch (UnknownHostException e)
                {
                    throw new IllegalArgumentException("Unknown host specified " + host, e);
                }
            }

            if (!specifiedHost.contains(FBUtilities.getBroadcastAddress()))
                throw new IllegalArgumentException("The current host must be part of the repair");

            if (specifiedHost.size() <= 1)
            {
                String msg = "Repair requires at least two endpoints that are neighbours before it can continue, the endpoint used for this repair is %s, " +
                             "other available neighbours are %s but these neighbours were not part of the supplied list of hosts to use during the repair (%s).";
                throw new IllegalArgumentException(String.format(msg, specifiedHost, neighbors, hosts));
            }

            specifiedHost.remove(FBUtilities.getBroadcastAddress());
            return specifiedHost;

        }

        return neighbors;
    }

    public synchronized UUID prepareForRepair(UUID parentRepairSession, Set<InetAddress> endpoints, RepairOption options,
                                              List<ColumnFamilyStore> columnFamilyStores, ListeningExecutorService executor)
    {
        long timestamp = System.currentTimeMillis();
        ParentRepairSession prs = registerParentRepairSession(parentRepairSession, columnFamilyStores, options.getRanges(), options.isIncremental(), timestamp, options.isGlobal(),
                                                              FBUtilities.getBroadcastAddress(), endpoints);
        prs.registerJobExecutor(executor);

        final CountDownLatch prepareLatch = new CountDownLatch(endpoints.size());
        final AtomicBoolean status = new AtomicBoolean(true);
        final Set<String> failedNodes = Collections.synchronizedSet(new HashSet<String>());
        IAsyncCallbackWithFailure callback = new IAsyncCallbackWithFailure()
        {
            public void response(MessageIn msg)
            {
                prepareLatch.countDown();
            }

            public boolean isLatencyForSnitch()
            {
                return false;
            }

            public void onFailure(InetAddress from)
            {
                status.set(false);
                failedNodes.add(from.getHostAddress());
                prepareLatch.countDown();
            }
        };

        List<UUID> cfIds = new ArrayList<>(columnFamilyStores.size());
        for (ColumnFamilyStore cfs : columnFamilyStores)
            cfIds.add(cfs.metadata.cfId);

        for (InetAddress neighbour : endpoints)
        {
            if (FailureDetector.instance.isAlive(neighbour))
            {
                PrepareMessage message = new PrepareMessage(parentRepairSession, cfIds, options.getRanges(), options.isIncremental(), timestamp, options.isGlobal());
                MessageOut<RepairMessage> msg = message.createMessage();
                MessagingService.instance().sendRR(msg, neighbour, callback, TimeUnit.HOURS.toMillis(1), true);
            }
            else
            {
                status.set(false);
                failedNodes.add(neighbour.getHostAddress());
                prepareLatch.countDown();
            }
        }
        try
        {
            prepareLatch.await(1, TimeUnit.HOURS);
        }
        catch (InterruptedException e)
        {
            parentRepairSessions.remove(parentRepairSession);
            throw new RuntimeException("Did not get replies from all endpoints. List of failed endpoint(s): " + failedNodes.toString(), e);
        }

        if (!status.get())
        {
            parentRepairSessions.remove(parentRepairSession);
            throw new RuntimeException("Did not get positive replies from all endpoints. List of failed endpoint(s): " + failedNodes.toString());
        }

        return parentRepairSession;
    }

    public ParentRepairSession registerParentRepairSession(UUID parentRepairSession, List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges,
                                                           boolean isIncremental, long timestamp, boolean isGlobal,
                                                           InetAddress coordinator, Set<InetAddress> participants)
    {
        ParentRepairSession prs = new ParentRepairSession(parentRepairSession, columnFamilyStores,
                                                            ranges, isIncremental, timestamp, isGlobal,
                                                            coordinator, participants);
        parentRepairSessions.put(parentRepairSession, prs);
        return prs;
    }

    public Set<SSTableReader> currentlyRepairing(UUID cfId, UUID parentRepairSession)
    {
        Set<SSTableReader> repairing = new HashSet<>();
        for (Map.Entry<UUID, ParentRepairSession> entry : parentRepairSessions.entrySet())
        {
            Collection<SSTableReader> sstables = entry.getValue().sstableMap.get(cfId);
            if (sstables != null && !entry.getKey().equals(parentRepairSession))
                repairing.addAll(sstables);
        }
        return repairing;
    }

    /**
     * Run final process of repair.
     * This removes all resources held by parent repair session, after performing anti compaction if necessary.
     *
     * @param parentSession Parent session ID
     * @param neighbors Repair participants (not including self)
     * @param successfulRanges Ranges that repaired successfully
     */
    public synchronized ListenableFuture finishParentSession(UUID parentSession, Set<InetAddress> neighbors, Collection<Range<Token>> successfulRanges)
    {
        ParentRepairSession prs = getParentRepairSession(parentSession);
        if (prs.isAborted())
        {
            String message = String.format("Parent repair session %s was aborted.", prs.id);
            logger.info(message);
            return Futures.immediateFailedFuture(new RuntimeException(message));
        }

        List<ListenableFuture<?>> tasks = new ArrayList<>(neighbors.size() + 1);
        for (InetAddress neighbor : neighbors)
        {
            AnticompactionTask task = new AnticompactionTask(parentSession, neighbor, successfulRanges);
            tasks.add(task);
            task.run(); // 'run' is just sending message
        }
        tasks.add(doAntiCompaction(parentSession, successfulRanges));
        return Futures.successfulAsList(tasks);
    }

    public void finishOngoingValidation(UUID parentSessionId, UUID cfId, UUID sessionId)
    {
        ParentRepairSession parentRepairSession = getParentRepairSession(parentSessionId);
        if (parentRepairSession != null)
            parentRepairSession.finishOngoingValidation(cfId, sessionId);
    }

    public void registerOngoingSync(UUID parentSessionId, UUID cfId, UUID sessionId, StreamResultFuture stream)
    {
        ParentRepairSession parentRepairSession = getParentRepairSession(parentSessionId);
        if (parentRepairSession != null)
            parentRepairSession.registerOngoingSync(cfId, sessionId, stream);
    }

    public void finishOngoingSync(UUID parentSessionId, UUID cfId, UUID sessionId)
    {
        ParentRepairSession parentRepairSession = getParentRepairSession(parentSessionId);
        if (parentRepairSession != null)
            parentRepairSession.finishOngoingSync(cfId, sessionId);
    }

    public ParentRepairSession getParentRepairSession(UUID parentSessionId)
    {
        return parentRepairSessions.get(parentSessionId);
    }

    public synchronized ParentRepairSession removeParentRepairSession(UUID parentSessionId)
    {
        return parentRepairSessions.remove(parentSessionId);
    }

    public synchronized boolean abortParentRepairSession(UUID parentSessionId, InetAddress aborter)
    {
        ParentRepairSession prs = getParentRepairSession(parentSessionId);
        if (prs == null)
        {
            logger.debug("Ignoring abort request for unknown parent repair session id: {}.", parentSessionId);
            return false;
        }

        logger.info("Aborting parent repair session {}", prs.id);

        boolean isCoordinator = FBUtilities.getBroadcastAddress().equals(prs.coordinator);

        if (isCoordinator)
        {
            for (InetAddress participant : prs.participants)
            {
                if (!participant.equals(prs.coordinator) && !participant.equals(aborter))
                {
                    logger.info("Sending abort message to participant {}", participant);
                    MessagingService.instance().sendOneWay(new AbortMessage(parentSessionId).createMessage(), participant);
                }
            }
        }
        else if (!aborter.equals(prs.coordinator))
        {
            logger.info("Sending abort message to coordinator {}", prs.coordinator);
            MessagingService.instance().sendOneWay(new AbortMessage(parentSessionId).createMessage(), prs.coordinator);
        }

        prs.abort();

        if (!isCoordinator)
        {
            removeParentRepairSession(parentSessionId);
        }
        return true;
    }

    /**
     * Submit anti-compaction jobs to CompactionManager.
     * When all jobs are done, parent repair session is removed whether those are suceeded or not.
     *
     * @param parentRepairSession parent repair session ID
     * @return Future result of all anti-compaction jobs.
     */
    @SuppressWarnings("resource")
    public ListenableFuture<List<Object>> doAntiCompaction(final UUID parentRepairSession, Collection<Range<Token>> successfulRanges)
    {
        assert parentRepairSession != null;
        ParentRepairSession prs = getParentRepairSession(parentRepairSession);
        //A repair will be marked as not global if it is a subrange repair to avoid many small anti-compactions
        //in addition to other scenarios such as repairs not involving all DCs or hosts
        if (!prs.isGlobal)
        {
            logger.info("Not a global repair, will not do anticompaction");
            if (!prs.isAborted())
                removeParentRepairSession(parentRepairSession);
            return Futures.immediateFuture(Collections.emptyList());
        }
        assert prs.ranges.containsAll(successfulRanges) : "Trying to perform anticompaction on unknown ranges";

        List<ListenableFuture<?>> futures = new ArrayList<>();
        // if we don't have successful repair ranges, then just skip anticompaction
        if (!successfulRanges.isEmpty())
        {
            for (Map.Entry<UUID, ColumnFamilyStore> columnFamilyStoreEntry : prs.columnFamilyStores.entrySet())
            {
                Refs<SSTableReader> sstables = prs.getAndReferenceSSTables(columnFamilyStoreEntry.getKey());
                ColumnFamilyStore cfs = columnFamilyStoreEntry.getValue();
                ListenableFuture<?> task = CompactionManager.instance.submitAntiCompaction(parentRepairSession, cfs, successfulRanges, sstables, prs.repairedAt);
                prs.registerOngoingAntiCompaction(task);
                task.addListener(new Runnable()
                {
                    public void run()
                    {
                        prs.finishOngoingAntiCompaction();
                    }
                }, MoreExecutors.sameThreadExecutor());
                futures.add(task);
            }
        }

        ListenableFuture<List<Object>> allAntiCompactionResults = Futures.successfulAsList(futures);
        allAntiCompactionResults.addListener(new Runnable()
        {
            @Override
            public void run()
            {
                ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(parentRepairSession);
                if (prs != null && !prs.isAborted())
                    removeParentRepairSession(parentRepairSession);
            }
        }, MoreExecutors.sameThreadExecutor());

        return allAntiCompactionResults;
    }

    /* Repair Message Handling */

    public void doPrepare(UUID parentSessionId, InetAddress coordinator, int callbackId, List<UUID> cfIds,
                          Collection<Range<Token>> ranges, boolean isIncremental, long timestamp, boolean isGlobal)
    {
        List<ColumnFamilyStore> columnFamilyStores = new ArrayList<>(cfIds.size());
        for (UUID cfId : cfIds)
        {
            ColumnFamilyStore columnFamilyStore = ColumnFamilyStore.getIfExists(cfId);
            if (columnFamilyStore == null)
            {
                logErrorAndSendFailureResponse(String.format("Table with id %s was dropped during prepare phase of repair",
                                                             cfId.toString()), coordinator, callbackId);
                return;
            }
            columnFamilyStores.add(columnFamilyStore);
        }
        registerParentRepairSession(parentSessionId, columnFamilyStores, ranges, isIncremental, timestamp, isGlobal, coordinator,
                                    Collections.EMPTY_SET);
        MessagingService.instance().sendReply(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), callbackId, coordinator);
    }

    public void doSnapshot(UUID parentSessionId, UUID sessionId, InetAddress coordinator, int callbackId, String ks, String table,
                           Collection<Range<Token>> ranges)
    {
        Pair<ParentRepairSession, ColumnFamilyStore> pair = validateAndUpdateStatus(parentSessionId, sessionId, ks, table, SessionStatus.SNAPSHOT,
                                                                          () -> logErrorAndSendFailureResponse(String.format("Table %s.%s was dropped during snapshot phase of repair",
                                                                                                                             ks, table), coordinator, callbackId));
        if (pair == null) return;

        ParentRepairSession parentRepairSession = pair.left;
        ColumnFamilyStore cfs = pair.right;

        final Collection<Range<Token>> repairingRange = ranges;
        Set<SSTableReader> snapshottedSSSTables = cfs.snapshot(sessionId.toString(), new Predicate<SSTableReader>()
        {
            public boolean apply(SSTableReader sstable)
            {
                return sstable != null &&
                       !sstable.metadata.isIndex() && // exclude SSTables from 2i
                       new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(repairingRange);
            }
        }, true, false); //ephemeral snapshot, if repair fails, it will be cleaned next startup

        if (parentRepairSession.isGlobal)
        {
            Set<SSTableReader> currentlyRepairing = currentlyRepairing(cfs.metadata.cfId, parentSessionId);
            if (!Sets.intersection(currentlyRepairing, snapshottedSSSTables).isEmpty())
            {
                // clear snapshot that we just created
                cfs.clearSnapshot(sessionId.toString());
                logErrorAndSendFailureResponse("Cannot start multiple repair sessions over the same sstables", coordinator, callbackId);
                return;
            }
            parentRepairSession.addSSTables(cfs.metadata.cfId, snapshottedSSSTables);
        }
        logger.debug("Enqueuing response to snapshot request {} to {}", sessionId, coordinator);
        MessagingService.instance().sendReply(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), callbackId, coordinator);
    }

    public void doValidate(UUID parentSessionId, UUID sessionId, InetAddress coordinator, RepairJobDesc desc, int gcBefore)
    {
        Pair<ParentRepairSession, ColumnFamilyStore> pair = validateAndUpdateStatus(parentSessionId, sessionId, desc.keyspace, desc.columnFamily, SessionStatus.VALIDATION,
                                                                                    () -> MessagingService.instance().sendOneWay(new ValidationComplete(desc).createMessage(),
                                                                                                                                 coordinator));
        if (pair == null) return;

        ParentRepairSession parentRepairSession = pair.left;
        ColumnFamilyStore store = pair.right;

        Validator validator = new Validator(desc, coordinator, gcBefore, store.metadata.cfId);
        Future<Object> future = CompactionManager.instance.submitValidation(store, validator);
        parentRepairSession.registerOngoingValidation(store.metadata.cfId, sessionId, future);
    }

    public void doSync(UUID parentSessionId, UUID sessionId, InetAddress coordinator, RepairJobDesc desc, SyncRequest request)
    {
        Pair<ParentRepairSession, ColumnFamilyStore> pair = validateAndUpdateStatus(parentSessionId, sessionId, desc.keyspace, desc.columnFamily, SessionStatus.SYNC,
                                                                                    () -> MessagingService.instance().sendOneWay(new SyncComplete(desc,
                                                                                                                                                  request.src,
                                                                                                                                                  request.dst, false).createMessage(),
                                                                                                                                 coordinator));
        if (pair == null) return;

        ParentRepairSession parentRepairSession = pair.left;
        ColumnFamilyStore cfs = pair.right;

        if (parentRepairSession == null) return;

        long repairedAt = parentRepairSession.getRepairedAt();

        StreamingRepairTask task = new StreamingRepairTask(desc, request, repairedAt, cfs.metadata.cfId);
        task.run();
    }

    public void doAbort(UUID parentSessionId, InetAddress from)
    {
        ParentRepairSession parentRepairSession = ActiveRepairService.instance.getParentRepairSession(parentSessionId);
        if (parentRepairSession == null)
        {
            logger.warn("Ignoring abort request for unknown parent repair session id: {}.", parentSessionId);
            return;
        }
        abortParentRepairSession(parentSessionId, from);
    }

    public void doAntiCompact(UUID parentSessionId, final InetAddress coordinator, final int callbackId,
                               Collection<Range<Token>> successfulRanges)
    {
        ListenableFuture<?> compactionDone = doAntiCompaction(parentSessionId, successfulRanges);
        Futures.addCallback(compactionDone, new FutureCallback<Object>()
        {
            public void onSuccess(Object o)
            {
                MessagingService.instance().sendReply(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), callbackId, coordinator);
            }

            public void onFailure(Throwable throwable)
            {
                ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(parentSessionId);
                if (prs != null && !prs.isAborted())
                {
                    logger.warn("Error during anti-compacton on repair session {}.", parentSessionId, throwable);
                    MessageOut reply = new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE)
                                       .withParameter(MessagingService.FAILURE_RESPONSE_PARAM, MessagingService.ONE_BYTE);
                    MessagingService.instance().sendReply(reply, callbackId, coordinator);
                }
                else
                {
                    logger.debug("Ignoring failure of anticompaction task from aborted or unkown parent repair session {}",
                                 parentSessionId, throwable);
                }
            }
        });
    }

    public void doCleanup(UUID parentSessionId, InetAddress coordinator, int messageId)
    {
        removeParentRepairSession(parentSessionId);
        MessagingService.instance().sendReply(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), messageId, coordinator);
    }

    public void doneSync(RepairJobDesc desc, NodePair nodes, boolean success)
    {
        RepairSession session = sessions.get(desc.sessionId);
        if (session == null)
            return;
        session.syncComplete(desc, nodes, success);
    }

    public void doneValidation(InetAddress endpoint, RepairJobDesc desc, MerkleTrees trees)
    {
        RepairSession session = sessions.get(desc.sessionId);
        if (session == null)
            return;
        session.validationComplete(desc, endpoint, trees);
    }

    /* End Repair Message Handling */

    private Pair<ParentRepairSession, ColumnFamilyStore> validateAndUpdateStatus(UUID parentSessionId, UUID sessionId, String ks, String table,
                                                                                 SessionStatus status, Runnable cfNotFoundAction)
    {
        ParentRepairSession parentRepairSession = ActiveRepairService.instance.getParentRepairSession(parentSessionId);
        if (parentRepairSession == null)
        {
            logger.warn("Ignoring {} request for unknown parent repair session id: {}.", status, parentSessionId);
            return null;
        }

        // trigger read-only compaction
        ColumnFamilyStore store = ColumnFamilyStore.getIfExists(ks, table);
        if (store == null)
        {
            logger.error("Table {}.{} was dropped during {} phase of repair", ks, table, status);
            cfNotFoundAction.run();
            return null;
        }

        parentRepairSession.updateSessionStatus(store.metadata.cfId, sessionId, status);
        return Pair.create(parentRepairSession, store);
    }

    private void logErrorAndSendFailureResponse(String errorMessage, InetAddress to, int id)
    {
        logger.error(errorMessage);
        MessageOut reply = new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE)
                           .withParameter(MessagingService.FAILURE_RESPONSE_PARAM, MessagingService.ONE_BYTE);
        MessagingService.instance().sendReply(reply, id, to);
    }

    public synchronized List<RepairInfo> listRepairs()
    {
        return parentRepairSessions.values().stream().map(r -> r.getRepairInfo()).collect(Collectors.toList());
    }

    enum SessionStatus
    {
        VALIDATION, SYNC, SNAPSHOT, WAITING_ANTICOMPACTION
    }

    public static class ParentRepairSession
    {
        private final Map<UUID, ColumnFamilyStore> columnFamilyStores = new HashMap<>();
        private final Collection<Range<Token>> ranges;
        private final Map<UUID, Set<SSTableReader>> sstableMap = new HashMap<>();
        private final long repairedAt;
        public final boolean isIncremental;
        public final boolean isGlobal;
        private final UUID id;
        private final Map<UUID, Map<UUID, SessionStatus>> sessionStatuses = new HashMap<>();
        protected final InetAddress coordinator;
        protected final Set<InetAddress> participants;
        private AtomicBoolean isAborted = new AtomicBoolean(false);

        private Map<UUID, Map<UUID, Future<Object>>> ongoingValidations = new HashMap<>();
        private Map<UUID, Map<UUID, StreamResultFuture>> ongoingSyncs = new HashMap<>();
        private ListenableFuture<?> ongoingAnticompaction = null;
        private Map<UUID, RepairSession> ongoingSessions = new HashMap<>();
        private ListeningExecutorService jobExecutor;

        public ParentRepairSession(UUID id, List<ColumnFamilyStore> columnFamilyStores,
                                   Collection<Range<Token>> ranges, boolean isIncremental,
                                   long repairedAt, boolean isGlobal, InetAddress coordinator,
                                   Set<InetAddress> participants)
        {
            this.id = id;
            for (ColumnFamilyStore cfs : columnFamilyStores)
            {
                this.columnFamilyStores.put(cfs.metadata.cfId, cfs);
                sstableMap.put(cfs.metadata.cfId, new HashSet<SSTableReader>());
                sessionStatuses.put(cfs.metadata.cfId, new HashMap<>());
                ongoingValidations.put(cfs.metadata.cfId, new HashMap<>());
                ongoingSyncs.put(cfs.metadata.cfId, new HashMap<>());
            }
            this.ranges = ranges;
            this.repairedAt = repairedAt;
            this.isIncremental = isIncremental;
            this.isGlobal = isGlobal;
            this.coordinator = coordinator;
            this.participants = participants;
        }

        public void addSSTables(UUID cfId, Set<SSTableReader> sstables)
        {
            sstableMap.get(cfId).addAll(sstables);
        }

        public synchronized void updateSessionStatus(UUID cfId, UUID sessionId, SessionStatus status)
        {
            sessionStatuses.get(cfId).put(sessionId, status);
        }

        @SuppressWarnings("resource")
        public synchronized Refs<SSTableReader> getAndReferenceSSTables(UUID cfId)
        {
            Set<SSTableReader> sstables = sstableMap.get(cfId);
            Iterator<SSTableReader> sstableIterator = sstables.iterator();
            ImmutableMap.Builder<SSTableReader, Ref<SSTableReader>> references = ImmutableMap.builder();
            while (sstableIterator.hasNext())
            {
                SSTableReader sstable = sstableIterator.next();
                if (!new File(sstable.descriptor.filenameFor(Component.DATA)).exists())
                {
                    sstableIterator.remove();
                }
                else
                {
                    Ref<SSTableReader> ref = sstable.tryRef();
                    if (ref == null)
                        sstableIterator.remove();
                    else
                        references.put(sstable, ref);
                }
            }
            return new Refs<>(references.build());
        }
        public long getRepairedAt()
        {
            if (isGlobal)
                return repairedAt;
            return ActiveRepairService.UNREPAIRED_SSTABLE;
        }
        @Override
        public String toString()
        {
            return "ParentRepairSession{" +
                    "columnFamilyStores=" + columnFamilyStores +
                    ", ranges=" + ranges +
                    ", sstableMap=" + sstableMap +
                    ", repairedAt=" + repairedAt +
                    '}';
        }

        public synchronized void registerOngoingValidation(UUID cfId, UUID sessionId, Future<Object> future)
        {
            ongoingValidations.get(cfId).put(sessionId, future);
        }

        public synchronized void finishOngoingValidation(UUID cfId, UUID sessionId)
        {
            Map<UUID, Future<Object>> cfValidations = ongoingValidations.get(cfId);
            if (cfValidations != null)
                cfValidations.remove(sessionId);
        }

        public synchronized void registerOngoingSync(UUID cfId, UUID sessionId, StreamResultFuture task)
        {
            ongoingSyncs.get(cfId).put(sessionId, task);
        }

        public synchronized void finishOngoingSync(UUID cfId, UUID sessionId)
        {
            Map<UUID, StreamResultFuture> cfSyncs = ongoingSyncs.get(cfId);
            if (cfSyncs == null)
                cfSyncs.remove(sessionId);
        }

        public synchronized void registerOngoingAntiCompaction(ListenableFuture<?> task)
        {
            ongoingAnticompaction = task;
        }

        public synchronized void finishOngoingAntiCompaction()
        {
            ongoingAnticompaction = null;
        }

        public void registerRepairSession(RepairSession session)
        {
            ongoingSessions.put(session.getId(), session);
        }

        public void registerJobExecutor(ListeningExecutorService executor)
        {
            this.jobExecutor = executor;
        }

        public boolean isAborted()
        {
            return isAborted.get();
        }

        public synchronized void abort()
        {
            isAborted.compareAndSet(false, true);
            abortJobExecutor();
            abortOngoingSessions();
            abortOngoingValidations();
            abortOngoingSyncs();
            abortOngoingAntiCompactions();
        }

        private void abortOngoingValidations()
        {
            for (Map.Entry<UUID, Map<UUID, Future<Object>>> activeTasksPerTable : ongoingValidations.entrySet())
            {
                for (Map.Entry<UUID, Future<Object>> activeTasks : activeTasksPerTable.getValue().entrySet())
                {
                    logger.debug("Aborting ongoing repair validation task {} of table {}.", activeTasks.getKey(), activeTasksPerTable.getKey());
                    activeTasks.getValue().cancel(true);
                }
            }
        }

        private void abortOngoingSyncs()
        {
            for (Map.Entry<UUID, Map<UUID, StreamResultFuture>> activeTasksPerTable : ongoingSyncs.entrySet())
            {
                for (Map.Entry<UUID, StreamResultFuture> activeTasks : activeTasksPerTable.getValue().entrySet())
                {
                    logger.debug("Aborting ongoing repair sync task {} of table {}.", activeTasks.getKey(), activeTasksPerTable.getKey());
                    activeTasks.getValue().cancel(true);
                }
            }
        }

        private void abortOngoingAntiCompactions()
        {
            if (ongoingAnticompaction != null)
            {
                logger.debug("Aborting ongoing anticompaction task of repair job {}.", id);
                ongoingAnticompaction.cancel(true);
            }
        }

        private void abortOngoingSessions()
        {
            for (Map.Entry<UUID, RepairSession> entry : ongoingSessions.entrySet())
            {
                logger.debug("Aborting ongoing repair session {} from parent job {}.", entry.getKey(), id);
                ongoingSessions.forEach((k, v) -> v.forceShutdown(new RuntimeException("Session is aborted.")));
            }
        }

        private void abortJobExecutor()
        {
            if (jobExecutor != null)
            {
                logger.debug("Shutting down job executor of repair job {}", id);
                jobExecutor.shutdownNow();
            }
        }

        public RepairInfo getRepairInfo()
        {
            RepairInfo info = new RepairInfo(id, coordinator);
            for (Map.Entry<UUID, Map<UUID, SessionStatus>> statusByCf : sessionStatuses.entrySet())
            {
                for (Map.Entry<UUID, SessionStatus> statusBySession : statusByCf.getValue().entrySet())
                {
                    info.addStatus(statusByCf.getKey(), statusBySession.getKey(), statusBySession.getValue().name());
                }
            }
            return info;
        }
    }
}
