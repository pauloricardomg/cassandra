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

package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BiMultiValMap;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SortedBiMultiValMap;

public class NewTokenMetadata implements ITokenMetadata
{
    private static final Logger logger = LoggerFactory.getLogger(NewTokenMetadata.class);

    public Ring ring = new Ring();

    /** Maintains endpoint to host ID map of every node in the cluster */
    private final BiMap<InetAddress, UUID> endpointToHostIdMap;

    private final Set<InetAddress> bootstrappingEndpoints = new HashSet<>();
    private final Set<InetAddress> movingEndpoints = new HashSet<>();
    private final Set<InetAddress> leavingEndpoints = new HashSet<>();

    // this is a cache of the calculation from {tokenToEndpointMap, bootstrapTokens, leavingEndpoints}
    private final ConcurrentMap<String, PendingRangeMaps> pendingRanges = new ConcurrentHashMap<String, PendingRangeMaps>();


    /* Use this lock for manipulating the token map */
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

    private final TokenMetadata.Topology topology;
    public final IPartitioner partitioner;

    private static final Comparator<InetAddress> inetaddressCmp = new Comparator<InetAddress>()
    {
        public int compare(InetAddress o1, InetAddress o2)
        {
            return ByteBuffer.wrap(o1.getAddress()).compareTo(ByteBuffer.wrap(o2.getAddress()));
        }
    };

    // signals replication strategies that nodes have joined or left the ring and they need to recompute ownership
    private volatile long ringVersion = 0;

    public NewTokenMetadata()
    {
        this(new Ring(),
             HashBiMap.<InetAddress, UUID>create(),
             new TokenMetadata.Topology(),
             DatabaseDescriptor.getPartitioner());
    }

    private NewTokenMetadata(Ring ring, BiMap<InetAddress, UUID> endpointsMap,
                             TokenMetadata.Topology topology, IPartitioner partitioner)
    {
        this.topology = topology;
        this.partitioner = partitioner;
        endpointToHostIdMap = endpointsMap;
        this.ring = ring;
    }

    private UUID lookupNodeId(InetAddress endpoint)
    {
        UUID nodeId = endpointToHostIdMap.get(endpoint);
        if (nodeId == null)
        {
            throw new RuntimeException(String.format("Could not find host id of endpoint %.", endpoint));
        }
        return nodeId;
    }

    /**
     * To be used by tests only (via {@link StorageService.setPartitionerUnsafe}).
     */
    @VisibleForTesting
    public NewTokenMetadata cloneWithNewPartitioner(IPartitioner newPartitioner)
    {
        return new NewTokenMetadata(ring, endpointToHostIdMap, topology, newPartitioner);
    }

    /** @return the number of nodes bootstrapping into source's primary range */
    public int pendingRangeChanges(InetAddress source)
    {
        int n = 0;
        Collection<Range<Token>> sourceRanges = getPrimaryRangesFor(getTokens(source));
        lock.readLock().lock();
        try
        {
            for (Token token : bootstrapTokens.keySet())
                for (Range<Token> range : sourceRanges)
                    if (range.contains(token))
                        n++;
        }
        finally
        {
            lock.readLock().unlock();
        }
        return n;
    }

    /**
     * Update token map with a single token/endpoint pair in normal state.
     */
    public void updateNormalToken(Token token, InetAddress endpoint)
    {
        updateNormalTokens(Collections.singleton(token), endpoint);
    }

    public void updateNormalTokens(Collection<Token> tokens, InetAddress endpoint)
    {
        Multimap<InetAddress, Token> endpointTokens = HashMultimap.create();
        for (Token token : tokens)
            endpointTokens.put(endpoint, token);
        updateNormalTokens(endpointTokens);
    }

    /**
     * Update token map with a set of token/endpoint pairs in normal state.
     *
     * Prefer this whenever there are multiple pairs to update, as each update (whether a single or multiple)
     * is expensive (CASSANDRA-3831).
     */
    public void updateNormalTokens(Multimap<InetAddress, Token> endpointTokens)
    {
        if (endpointTokens.isEmpty())
            return;

        lock.writeLock().lock();
        try
        {
            for (InetAddress endpoint : endpointTokens.keySet())
            {
                Collection<Token> tokens = endpointTokens.get(endpoint);

                assert tokens != null && !tokens.isEmpty();

                removeFromPending(endpoint);

                topology.addEndpoint(endpoint);
                removeFromMoving(endpoint); // also removing this endpoint from moving

                UUID nodeId = lookupNodeId(endpoint);

                for (Token token : tokens)
                    ring.setNormal(nodeId, token);
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Store an end-point to host ID mapping.  Each ID must be unique, and
     * cannot be changed after the fact.
     */
    public void updateHostId(UUID hostId, InetAddress endpoint)
    {
        assert hostId != null;
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            InetAddress storedEp = endpointToHostIdMap.inverse().get(hostId);
            if (storedEp != null)
            {
                if (!storedEp.equals(endpoint) && (FailureDetector.instance.isAlive(storedEp)))
                {
                    throw new RuntimeException(String.format("Host ID collision between active endpoint %s and %s (id=%s)",
                                                             storedEp,
                                                             endpoint,
                                                             hostId));
                }
            }

            UUID storedId = endpointToHostIdMap.get(endpoint);
            if ((storedId != null) && (!storedId.equals(hostId)))
                logger.warn("Changing {}'s host ID from {} to {}", endpoint, storedId, hostId);
    
            endpointToHostIdMap.forcePut(endpoint, hostId);
        }
        finally
        {
            lock.writeLock().unlock();
        }

    }

    /** Return the unique host ID for an end-point. */
    public UUID getHostId(InetAddress endpoint)
    {
        lock.readLock().lock();
        try
        {
            return endpointToHostIdMap.get(endpoint);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /** Return the end-point for a unique host ID */
    public InetAddress getEndpointForHostId(UUID hostId)
    {
        lock.readLock().lock();
        try
        {
            return endpointToHostIdMap.inverse().get(hostId);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /** @return a copy of the endpoint-to-id map for read-only operations */
    public Map<InetAddress, UUID> getEndpointToHostIdMapForReading()
    {
        lock.readLock().lock();
        try
        {
            Map<InetAddress, UUID> readMap = new HashMap<>();
            readMap.putAll(endpointToHostIdMap);
            return readMap;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Deprecated
    public void addBootstrapToken(Token token, InetAddress endpoint)
    {
        addBootstrapTokens(Collections.singleton(token), endpoint);
    }

    public void addBootstrapTokens(Collection<Token> tokens, InetAddress endpoint)
    {
        assert tokens != null && !tokens.isEmpty();
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            UUID nodeId = lookupNodeId(endpoint);
            for (Token token : tokens)
                ring.setAdding(nodeId, token);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    @Deprecated
    public void removeBootstrapTokens(Collection<Token> tokens)
    {
        // not needed - removeEndpoint is called before
    }

    public void addLeavingEndpoint(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            leavingEndpoints.add(endpoint);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Add a new moving endpoint
     * @param token token which is node moving to
     * @param endpoint address of the moving node
     */
    public void addMovingEndpoint(Token token, InetAddress endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();

        try
        {
            UUID nodeId = lookupNodeId(endpoint);
            Collection<Token> ownedTokens = ring.getTokens(nodeId);
            assert ownedTokens.size() == 1 : "Can only move nodes with only 1 token";
            ring.setRemoving(nodeId, ownedTokens.iterator().next());
            ring.setAdding(nodeId, token);
            movingEndpoints.add(endpoint);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public void removeEndpoint(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            UUID nodeId = lookupNodeId(endpoint);
            topology.removeEndpoint(endpoint);
            removeFromPending(endpoint);
            endpointToHostIdMap.remove(endpoint);
            ring.removeNode(nodeId);
            invalidateCachedRings();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    private void removeFromPending(InetAddress endpoint)
    {
        leavingEndpoints.remove(endpoint);
        movingEndpoints.remove(endpoint);
        bootstrappingEndpoints.remove(endpoint);
    }

    /**
     * This is called when the snitch properties for this endpoint are updated, see CASSANDRA-10238.
     */
    public void updateTopology(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            logger.info("Updating topology for {}", endpoint);
            topology.updateEndpoint(endpoint);
            invalidateCachedRings();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * This is called when the snitch properties for many endpoints are updated, it will update
     * the topology mappings of any endpoints whose snitch has changed, see CASSANDRA-10238.
     */
    public void updateTopology()
    {
        lock.writeLock().lock();
        try
        {
            logger.info("Updating topology for all endpoints that have changed");
            topology.updateEndpoints();
            invalidateCachedRings();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove pair of token/address from moving endpoints
     * @param endpoint address of the moving node
     */
    public void removeFromMoving(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            UUID nodeId = lookupNodeId(endpoint);
            ring.removePendingTokens(nodeId);
            invalidateCachedRings();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public Collection<Token> getTokens(InetAddress endpoint)
    {
        assert endpoint != null;
        assert isMember(endpoint); // don't want to return nulls

        lock.readLock().lock();
        try
        {
            UUID nodeId = lookupNodeId(endpoint);
            return ring.getTokens(nodeId);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Deprecated
    public Token getToken(InetAddress endpoint)
    {
        return getTokens(endpoint).iterator().next();
    }

    public boolean isMember(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();
        try
        {
            UUID nodeId = lookupNodeId(endpoint);
            return ring.hasNormalToken(nodeId);        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public boolean isLeaving(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();
        try
        {
            return leavingEndpoints.contains(endpoint);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public boolean isMoving(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();

        try
        {
            return movingEndpoints.contains(endpoint);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    private final AtomicReference<NewTokenMetadata> cachedTokenMap = new AtomicReference<>();

    /**
     * Create a copy of TokenMetadata with only tokenToEndpointMap. That is, pending ranges,
     * bootstrap tokens and leaving endpoints are not included in the copy.
     */
    public NewTokenMetadata cloneOnlyTokenMap()
    {
        lock.readLock().lock();
        try
        {
            return new NewTokenMetadata(ring,
                                     HashBiMap.create(endpointToHostIdMap),
                                     new TokenMetadata.Topology(topology),
                                     partitioner);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * Return a cached TokenMetadata with only tokenToEndpointMap, i.e., the same as cloneOnlyTokenMap but
     * uses a cached copy that is invalided when the ring changes, so in the common case
     * no extra locking is required.
     *
     * Callers must *NOT* mutate the returned metadata object.
     */
    public NewTokenMetadata cachedOnlyTokenMap()
    {
        NewTokenMetadata tm = cachedTokenMap.get();
        if (tm != null)
            return tm;

        // synchronize to prevent thundering herd (CASSANDRA-6345)
        synchronized (this)
        {
            if ((tm = cachedTokenMap.get()) != null)
                return tm;

            tm = cloneOnlyTokenMap();
            cachedTokenMap.set(tm);
            return tm;
        }
    }

    /**
     * Create a copy of TokenMetadata with tokenToEndpointMap reflecting situation after all
     * current leave operations have finished.
     *
     * @return new token metadata
     */
    public NewTokenMetadata cloneAfterAllLeft()
    {
        lock.readLock().lock();
        try
        {
            NewTokenMetadata allLeftMetadata = cloneOnlyTokenMap();

            for (InetAddress endpoint : leavingEndpoints)
                allLeftMetadata.removeEndpoint(endpoint);

            return allLeftMetadata;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * Create a copy of TokenMetadata with tokenToEndpointMap reflecting situation after all
     * current leave, and move operations have finished.
     *
     * @return new token metadata
     */
    public NewTokenMetadata cloneAfterAllSettled()
    {
        lock.readLock().lock();

        try
        {
            NewTokenMetadata metadata = cloneOnlyTokenMap();

            for (InetAddress endpoint : leavingEndpoints)
                metadata.removeEndpoint(endpoint);


            for (Pair<Token, InetAddress> pair : movingEndpoints)
                metadata.updateNormalToken(pair.left, pair.right);

            return metadata;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public InetAddress getEndpoint(Token token)
    {
        lock.readLock().lock();
        try
        {
            Set<UUID> nonPendingOwners = ring.getNonPendingOwners(token);
            if (!nonPendingOwners.isEmpty())
            {
                UUID firstNonPendingOwner = nonPendingOwners.iterator().next();
                return endpointToHostIdMap.inverse().get(firstNonPendingOwner);
            }
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public Collection<Range<Token>> getPrimaryRangesFor(Collection<Token> tokens)
    {
        Collection<Range<Token>> ranges = new ArrayList<>(tokens.size());
        for (Token right : tokens)
            ranges.add(new Range<>(getPredecessor(right), right));
        return ranges;
    }

    @Deprecated
    public Range<Token> getPrimaryRangeFor(Token right)
    {
        return getPrimaryRangesFor(Arrays.asList(right)).iterator().next();
    }

    public List<Token> sortedTokens()
    {
        return ring.getNonPendingTokens();
    }

    public Multimap<Range<Token>, InetAddress> getPendingRangesMM(String keyspaceName)
    {
        Multimap<Range<Token>, InetAddress> map = HashMultimap.create();
        PendingRangeMaps pendingRangeMaps = this.pendingRanges.get(keyspaceName);

        if (pendingRangeMaps != null)
        {
            for (Map.Entry<Range<Token>, List<InetAddress>> entry : pendingRangeMaps)
            {
                Range<Token> range = entry.getKey();
                for (InetAddress address : entry.getValue())
                {
                    map.put(range, address);
                }
            }
        }

        return map;
    }

    /** a mutable map may be returned but caller should not modify it */
    public PendingRangeMaps getPendingRanges(String keyspaceName)
    {
        return this.pendingRanges.get(keyspaceName);
    }

    public List<Range<Token>> getPendingRanges(String keyspaceName, InetAddress endpoint)
    {
        List<Range<Token>> ranges = new ArrayList<>();
        for (Map.Entry<Range<Token>, InetAddress> entry : getPendingRangesMM(keyspaceName).entries())
        {
            if (entry.getValue().equals(endpoint))
            {
                ranges.add(entry.getKey());
            }
        }
        return ranges;
    }

     /**
     * Calculate pending ranges according to bootsrapping and leaving nodes. Reasoning is:
     *
     * (1) When in doubt, it is better to write too much to a node than too little. That is, if
     * there are multiple nodes moving, calculate the biggest ranges a node could have. Cleaning
     * up unneeded data afterwards is better than missing writes during movement.
     * (2) When a node leaves, ranges for other nodes can only grow (a node might get additional
     * ranges, but it will not lose any of its current ranges as a result of a leave). Therefore
     * we will first remove _all_ leaving tokens for the sake of calculation and then check what
     * ranges would go where if all nodes are to leave. This way we get the biggest possible
     * ranges with regard current leave operations, covering all subsets of possible final range
     * values.
     * (3) When a node bootstraps, ranges of other nodes can only get smaller. Without doing
     * complex calculations to see if multiple bootstraps overlap, we simply base calculations
     * on the same token ring used before (reflecting situation after all leave operations have
     * completed). Bootstrapping nodes will be added and removed one by one to that metadata and
     * checked what their ranges would be. This will give us the biggest possible ranges the
     * node could have. It might be that other bootstraps make our actual final ranges smaller,
     * but it does not matter as we can clean up the data afterwards.
     *
     * NOTE: This is heavy and ineffective operation. This will be done only once when a node
     * changes state in the cluster, so it should be manageable.
     */
    public void calculatePendingRanges(AbstractReplicationStrategy strategy, String keyspaceName)
    {
        lock.readLock().lock();
        try
        {
            PendingRangeMaps newPendingRanges = new PendingRangeMaps();

            if (bootstrappingEndpoints.isEmpty() && leavingEndpoints.isEmpty() && movingEndpoints.isEmpty())
            {
                if (logger.isTraceEnabled())
                    logger.trace("No bootstrapping, leaving or moving nodes -> empty pending ranges for {}", keyspaceName);

                pendingRanges.put(keyspaceName, newPendingRanges);
                return;
            }

            Multimap<InetAddress, Range<Token>> addressRanges = strategy.getAddressRanges();

            // Copy of metadata reflecting the situation after all leave operations are finished.
            NewTokenMetadata allLeftMetadata = cloneAfterAllLeft();

            // get all ranges that will be affected by leaving nodes
            Set<Range<Token>> affectedRanges = new HashSet<Range<Token>>();
            for (InetAddress endpoint : leavingEndpoints)
                affectedRanges.addAll(addressRanges.get(endpoint));

            // for each of those ranges, find what new nodes will be responsible for the range when
            // all leaving nodes are gone.
            NewTokenMetadata metadata = cloneOnlyTokenMap(); // don't do this in the loop! #7758
            for (Range<Token> range : affectedRanges)
            {
                Set<InetAddress> currentEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(range.right, metadata));
                Set<InetAddress> newEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(range.right, allLeftMetadata));
                for (InetAddress address : Sets.difference(newEndpoints, currentEndpoints))
                {
                    newPendingRanges.addPendingRange(range, address);
                }
            }

            // At this stage newPendingRanges has been updated according to leave operations. We can
            // now continue the calculation by checking bootstrapping nodes.

            // For each of the bootstrapping nodes, simply add and remove them one by one to
            // allLeftMetadata and check in between what their ranges would be.
            Multimap<InetAddress, Token> bootstrapAddresses = bootstrapTokens.inverse();
            for (InetAddress endpoint : bootstrapAddresses.keySet())
            {
                Collection<Token> tokens = bootstrapAddresses.get(endpoint);

                allLeftMetadata.updateNormalTokens(tokens, endpoint);
                for (Range<Token> range : strategy.getAddressRanges(allLeftMetadata).get(endpoint))
                {
                    newPendingRanges.addPendingRange(range, endpoint);
                }
                allLeftMetadata.removeEndpoint(endpoint);
            }

            // At this stage newPendingRanges has been updated according to leaving and bootstrapping nodes.
            // We can now finish the calculation by checking moving nodes.

            // For each of the moving nodes, we do the same thing we did for bootstrapping:
            // simply add and remove them one by one to allLeftMetadata and check in between what their ranges would be.
            for (Pair<Token, InetAddress> moving : movingEndpoints)
            {
                //Calculate all the ranges which will could be affected. This will include the ranges before and after the move.
                Set<Range<Token>> moveAffectedRanges = new HashSet<>();
                InetAddress endpoint = moving.right; // address of the moving node
                //Add ranges before the move
                for (Range<Token> range : strategy.getAddressRanges(allLeftMetadata).get(endpoint))
                {
                    moveAffectedRanges.add(range);
                }

                allLeftMetadata.updateNormalToken(moving.left, endpoint);
                //Add ranges after the move
                for (Range<Token> range : strategy.getAddressRanges(allLeftMetadata).get(endpoint))
                {
                    moveAffectedRanges.add(range);
                }

                for(Range<Token> range : moveAffectedRanges)
                {
                    Set<InetAddress> currentEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(range.right, metadata));
                    Set<InetAddress> newEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(range.right, allLeftMetadata));
                    Set<InetAddress> difference = Sets.difference(newEndpoints, currentEndpoints);
                    for(final InetAddress address : difference)
                    {
                        Collection<Range<Token>> newRanges = strategy.getAddressRanges(allLeftMetadata).get(address);
                        Collection<Range<Token>> oldRanges = strategy.getAddressRanges(metadata).get(address);
                        //We want to get rid of any ranges which the node is currently getting.
                        newRanges.removeAll(oldRanges);

                        for(Range<Token> newRange : newRanges)
                        {
                            for(Range<Token> pendingRange : newRange.subtractAll(oldRanges))
                            {
                                newPendingRanges.addPendingRange(pendingRange, address);
                            }
                        }
                    }
                }

                allLeftMetadata.removeEndpoint(endpoint);
            }

            pendingRanges.put(keyspaceName, newPendingRanges);

            if (logger.isTraceEnabled())
                logger.trace("Pending ranges:\n{}", (pendingRanges.isEmpty() ? "<empty>" : printPendingRanges()));
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public Token getPredecessor(Token token)
    {
        return ring.getPredecessor(token);
    }

    /** @return a copy of the bootstrapping tokens map */
    public Set<InetAddress> getBootstrappingEndpoints()
    {
        lock.readLock().lock();
        try
        {
            return ImmutableSet.copyOf((bootstrappingEndpoints);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public Set<InetAddress> getAllEndpoints()
    {
        lock.readLock().lock();
        try
        {
            return ImmutableSet.copyOf(endpointToHostIdMap.keySet());
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /** caller should not modify leavingEndpoints */
    public Set<InetAddress> getLeavingEndpoints()
    {
        lock.readLock().lock();
        try
        {
            return ImmutableSet.copyOf(leavingEndpoints);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * Endpoints which are migrating to the new tokens
     * @return set of addresses of moving endpoints
     */
    public Set<InetAddress> getMovingEndpoints()
    {
        lock.readLock().lock();
        try
        {
            return ImmutableSet.copyOf(movingEndpoints);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public static int firstTokenIndex(final List<Token> ring, Token start, boolean insertMin)
    {
        assert ring.size() > 0;
        // insert the minimum token (at index == -1) if we were asked to include it and it isn't a member of the ring
        int i = Collections.binarySearch(ring, start);
        if (i < 0)
        {
            i = (i + 1) * (-1);
            if (i >= ring.size())
                i = insertMin ? -1 : 0;
        }
        return i;
    }

    public static Token firstToken(final List<Token> ring, Token start)
    {
        return ring.get(firstTokenIndex(ring, start, false));
    }

    /**
     * iterator over the Tokens in the given ring, starting with the token for the node owning start
     * (which does not have to be a Token in the ring)
     * @param includeMin True if the minimum token should be returned in the ring even if it has no owner.
     */
    public static Iterator<Token> ringIterator(final List<Token> ring, Token start, boolean includeMin)
    {
        if (ring.isEmpty())
            return includeMin ? Iterators.singletonIterator(start.getPartitioner().getMinimumToken())
                              : Collections.emptyIterator();

        final boolean insertMin = includeMin && !ring.get(0).isMinimum();
        final int startIndex = firstTokenIndex(ring, start, insertMin);
        return new AbstractIterator<Token>()
        {
            int j = startIndex;
            protected Token computeNext()
            {
                if (j < -1)
                    return endOfData();
                try
                {
                    // return minimum for index == -1
                    if (j == -1)
                        return start.getPartitioner().getMinimumToken();
                    // return ring token for other indexes
                    return ring.get(j);
                }
                finally
                {
                    j++;
                    if (j == ring.size())
                        j = insertMin ? -1 : 0;
                    if (j == startIndex)
                        // end iteration
                        j = -2;
                }
            }
        };
    }

    /** used by tests */
    public void clearUnsafe()
    {
        lock.writeLock().lock();
        try
        {
            ring.clear();
            endpointToHostIdMap.clear();
            leavingEndpoints.clear();
            bootstrappingEndpoints.clear();
            pendingRanges.clear();
            movingEndpoints.clear();
            topology.clear();
            invalidateCachedRings();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        lock.readLock().lock();
        try
        {
            List<Token> nonPendingTokens = ring.getNonPendingTokens();
            Set<InetAddress> eps = nonPendingTokens.stream().map(i -> endpointToHostIdMap.inverse().get(i)).filter(i -> i != null).collect(Collectors.toSet());

            if (!eps.isEmpty())
            {
                sb.append("Normal Tokens:");
                sb.append(System.getProperty("line.separator"));
                for (InetAddress ep : eps)
                {
                    sb.append(ep);
                    sb.append(':');
                    sb.append(tokenToEndpointMap.inverse().get(ep));
                    sb.append(System.getProperty("line.separator"));
                }
            }

            if (!bootstrapTokens.isEmpty())
            {
                sb.append("Bootstrapping Tokens:" );
                sb.append(System.getProperty("line.separator"));
                for (Map.Entry<Token, InetAddress> entry : bootstrapTokens.entrySet())
                {
                    sb.append(entry.getValue()).append(':').append(entry.getKey());
                    sb.append(System.getProperty("line.separator"));
                }
            }

            if (!leavingEndpoints.isEmpty())
            {
                sb.append("Leaving Endpoints:");
                sb.append(System.getProperty("line.separator"));
                for (InetAddress ep : leavingEndpoints)
                {
                    sb.append(ep);
                    sb.append(System.getProperty("line.separator"));
                }
            }

            if (!pendingRanges.isEmpty())
            {
                sb.append("Pending Ranges:");
                sb.append(System.getProperty("line.separator"));
                sb.append(printPendingRanges());
            }
        }
        finally
        {
            lock.readLock().unlock();
        }

        return sb.toString();
    }

    private String printPendingRanges()
    {
        StringBuilder sb = new StringBuilder();

        for (PendingRangeMaps pendingRangeMaps : pendingRanges.values())
        {
            sb.append(pendingRangeMaps.printPendingRanges());
        }

        return sb.toString();
    }

    public Collection<InetAddress> pendingEndpointsFor(Token token, String keyspaceName)
    {
        PendingRangeMaps pendingRangeMaps = this.pendingRanges.get(keyspaceName);
        if (pendingRangeMaps == null)
            return Collections.emptyList();

        return pendingRangeMaps.pendingEndpointsFor(token);
    }

    /**
     * @deprecated retained for benefit of old tests
     */
    public Collection<InetAddress> getWriteEndpoints(Token token, String keyspaceName, Collection<InetAddress> naturalEndpoints)
    {
        return ImmutableList.copyOf(Iterables.concat(naturalEndpoints, pendingEndpointsFor(token, keyspaceName)));
    }

    /** @return an endpoint to token multimap representation of tokenToEndpointMap (a copy) */
    public Multimap<InetAddress, Token> getEndpointToTokenMapForReading()
    {
        lock.readLock().lock();
        try
        {
            Multimap<InetAddress, Token> cloned = HashMultimap.create();
            for (Map.Entry<Token, InetAddress> entry : tokenToEndpointMap.entrySet())
                cloned.put(entry.getValue(), entry.getKey());
            return cloned;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * @return a (stable copy, won't be modified) Token to Endpoint map for all the normal and bootstrapping nodes
     *         in the cluster.
     */
    public Map<Token, InetAddress> getNormalAndBootstrappingTokenToEndpointMap()
    {
        lock.readLock().lock();
        try
        {
            Map<Token, InetAddress> map = new HashMap<>(tokenToEndpointMap.size() + bootstrapTokens.size());
            map.putAll(tokenToEndpointMap);
            map.putAll(bootstrapTokens);
            return map;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * @return the Topology map of nodes to DCs + Racks
     *
     * This is only allowed when a copy has been made of TokenMetadata, to avoid concurrent modifications
     * when Topology methods are subsequently used by the caller.
     */
    public TokenMetadata.Topology getTopology()
    {
        assert this != StorageService.instance.getTokenMetadata();
        return topology;
    }

    public long getRingVersion()
    {
        return ringVersion;
    }

    public void invalidateCachedRings()
    {
        ringVersion++;
        cachedTokenMap.set(null);
    }

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return partitioner.decorateKey(key);
    }
}
