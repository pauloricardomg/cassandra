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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.BiMap;
import com.google.common.collect.Multimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.utils.BiMultiValMap;
import org.apache.cassandra.utils.Pair;

public class ITokenMetadataImpl
{
    private static final Logger logger = LoggerFactory.getLogger(ITokenMetadataImpl.class);

    private Ring ring = new Ring();

    /** Maintains endpoint to host ID map of every node in the cluster */
    private final BiMap<InetAddress, UUID> endpointToHostIdMap = null;

    /* Use this lock for manipulating the token map */
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

    public TokenMetadata cloneWithNewPartitioner(IPartitioner newPartitioner)
    {
        return null;
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

    public void updateNormalToken(Token token, InetAddress endpoint)
    {
        UUID nodeId = lookupNodeId(endpoint);
        ring.setNormal(nodeId, token);
    }

    public void updateNormalTokens(Collection<Token> tokens, InetAddress endpoint)
    {
        UUID nodeId = lookupNodeId(endpoint);

        for (Token token : tokens)
            ring.setNormal(nodeId, token);
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

    public InetAddress getEndpointForHostId(UUID hostId)
    {
        return endpointToHostIdMap.inverse().get(hostId);
    }

    public Map<InetAddress, UUID> getEndpointToHostIdMapForReading()
    {
        return null;
    }

    public void addBootstrapToken(Token token, InetAddress endpoint)
    {
        UUID nodeId = lookupNodeId(endpoint);
        ring.setAdding(nodeId, token);
    }

    public void addBootstrapTokens(Collection<Token> tokens, InetAddress endpoint)
    {
        UUID nodeId = lookupNodeId(endpoint);
        for (Token token : tokens)
            ring.setAdding(nodeId, token);
    }

    public void removeBootstrapTokens(Collection<Token> tokens)
    {

    }

    public void addLeavingEndpoint(InetAddress endpoint)
    {
        UUID nodeId = lookupNodeId(endpoint);
        ring.setRemoving(nodeId);
    }

    public void addMovingEndpoint(Token token, InetAddress endpoint)
    {
        UUID nodeId = lookupNodeId(endpoint);
        Collection<Token> ownedTokens = ring.getTokens(nodeId);
        assert ownedTokens.size() == 1 : "Can only move nodes with only 1 token";
        ring.setRemoving(nodeId, ownedTokens.iterator().next());
        ring.setAdding(nodeId, token);
    }

    public void removeEndpoint(InetAddress endpoint)
    {
        UUID nodeId = lookupNodeId(endpoint);
        ring.removeNode(nodeId);
    }

    public void updateTopology(InetAddress endpoint)
    {

    }

    public void updateTopology()
    {

    }

    public void removeFromMoving(InetAddress endpoint)
    {
        UUID nodeId = lookupNodeId(endpoint);
        ring.removePendingTokens(nodeId);
    }

    public Collection<Token> getTokens(InetAddress endpoint)
    {
        UUID nodeId = lookupNodeId(endpoint);
        return ring.getTokens(nodeId);
    }

    public Token getToken(InetAddress endpoint)
    {
        return getTokens(endpoint).iterator().next();
    }

    public boolean isMember(InetAddress endpoint)
    {
        UUID nodeId = lookupNodeId(endpoint);
        return ring.hasNormalToken(nodeId);
    }

    public boolean isLeaving(InetAddress endpoint)
    {
        UUID nodeId = lookupNodeId(endpoint);
        return ring.isRemovingToken(nodeId);
    }

    public boolean isMoving(InetAddress endpoint)
    {
        UUID nodeId = lookupNodeId(endpoint);
        return ring.isAddingToken(nodeId) && ring.isRemovingToken(nodeId);
    }

    public TokenMetadata cloneOnlyTokenMap()
    {
        return null;
    }

    public TokenMetadata cachedOnlyTokenMap()
    {
        return null;
    }

    public TokenMetadata cloneAfterAllLeft()
    {
        return null;
    }

    public TokenMetadata cloneAfterAllSettled()
    {
        return null;
    }

    public InetAddress getEndpoint(Token token)
    {
        Set<UUID> nonPendingOwners = ring.getNonPendingOwners(token);
        if (!nonPendingOwners.isEmpty())
        {
            UUID firstNonPendingOwner = nonPendingOwners.iterator().next();
            return endpointToHostIdMap.inverse().get(firstNonPendingOwner);
        }
        return null;
    }

    public Collection<Range<Token>> getPrimaryRangesFor(Collection<Token> tokens)
    {
        Collection<Range<Token>> ranges = new ArrayList<>(tokens.size());
        for (Token right : tokens)
            ranges.add(new Range<>(getPredecessor(right), right));
        return ranges;
    }

    public Range<Token> getPrimaryRangeFor(Token right)
    {
        return getPrimaryRangesFor(Collections.singletonList(right)).iterator().next();
    }

    public List<Token> sortedTokens()
    {
        return ring.getNonPendingTokens();
    }

    public Multimap<Range<Token>, InetAddress> getPendingRangesMM(String keyspaceName)
    {
        return null;
    }

    public PendingRangeMaps getPendingRanges(String keyspaceName)
    {
        return null;
    }

    public List<Range<Token>> getPendingRanges(String keyspaceName, InetAddress endpoint)
    {
        return null;
    }

    public void calculatePendingRanges(AbstractReplicationStrategy strategy, String keyspaceName)
    {

    }

    public Token getPredecessor(Token token)
    {
        return ring.getPredecessor(token);
    }

    public BiMultiValMap<Token, InetAddress> getBootstrapTokens()
    {
        Map<Token, UUID> addingTokens = ring.getAddingTokens();
        BiMultiValMap<Token, InetAddress> result = new BiMultiValMap<>();

        for (Map.Entry<Token, UUID> entry : addingTokens.entrySet())
        {
            InetAddress endpoint = endpointToHostIdMap.inverse().get(entry.getValue());
            result.put(entry.getKey(), endpoint);
        }

        return result;
    }

    public Set<InetAddress> getAllEndpoints()
    {
        return null;
    }

    public Set<InetAddress> getLeavingEndpoints()
    {
        return null;
    }

    public Set<Pair<Token, InetAddress>> getMovingEndpoints()
    {
        return null;
    }

    public void clearUnsafe()
    {

    }

    public Collection<InetAddress> pendingEndpointsFor(Token token, String keyspaceName)
    {
        return null;
    }

    public Collection<InetAddress> getWriteEndpoints(Token token, String keyspaceName, Collection<InetAddress> naturalEndpoints)
    {
        return null;
    }

    public Multimap<InetAddress, Token> getEndpointToTokenMapForReading()
    {
        return null;
    }

    public Map<Token, InetAddress> getNormalAndBootstrappingTokenToEndpointMap()
    {
        return null;
    }

    public TokenMetadata.Topology getTopology()
    {
        return null;
    }

    public long getRingVersion()
    {
        return 0;
    }

    public void invalidateCachedRings()
    {

    }

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return null;
    }
}
