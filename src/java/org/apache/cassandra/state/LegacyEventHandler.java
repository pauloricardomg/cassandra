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

package org.apache.cassandra.state;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.TokenSerializer;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.state.legacy.LegacyEvent;
import org.apache.cassandra.state.legacy.MoveEndpointEvent;

public class LegacyEventHandler implements IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(LegacyEventHandler.class);

    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;

    private final Multimap<InetAddressAndPort, Token> ipToTokens = Multimaps.synchronizedMultimap(HashMultimap.create());
    private final Map<Token, InetAddressAndPort> tokenToIp = new ConcurrentHashMap<>();
    private final Map<InetAddressAndPort, UUID> ipToUUID = new ConcurrentHashMap<>();

    private final Set<LegacyEvent> undelivered = Collections.synchronizedSet(new LinkedHashSet<>());

    public final TokenRing ring = new TokenRing();

    public final IPartitioner partitioner;

    public LegacyEventHandler(IPartitioner partitioner) {
        this.partitioner = partitioner;
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
    }

    public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
    {
        switch (state) {
            case HOST_ID:
                Optional<ClusterSnapshot> updated = updateHostId(endpoint, value);
                updated.ifPresent(s -> onClusterChanged(s));
                break;

            case TOKENS:
                updated = updateTokens(endpoint, value);
                updated.ifPresent(s -> onClusterChanged(s));

            case STATUS:
            case STATUS_WITH_PORT:
                LegacyEvent event = LegacyEvent.get(endpoint, value, partitioner);

                if (event instanceof MoveEndpointEvent)
                {
                    //TODO remove temporary token when node dies before move completes
                    //TODO same for bootstrap, replace etc
                    MoveEndpointEvent moveEvent = (MoveEndpointEvent)event;
                    updated = addToken(endpoint, moveEvent.getMoveToken());
                    updated.ifPresent(s -> onClusterChanged(s));
                }

                maybeDispatch(event);
                break;
        }
    }

    private Optional<ClusterSnapshot> addToken(InetAddressAndPort endpoint, Token token)
    {
        readLock.lock();
        try
        {
            if (endpoint.equals(tokenToIp.get(token)))
                return Optional.empty();
        }
        finally
        {
            readLock.unlock();
        }

        writeLock.lock();
        try
        {
            InetAddressAndPort prev = tokenToIp.put(token, endpoint);
            if (prev != null)
            {
                logger.warn("Token {} moving from {} to {}.", token, prev, endpoint);
                ipToTokens.remove(prev, token);
            }
            ipToTokens.put(endpoint, token);
            return Optional.of(getClusterSnapshot());
        }
        finally
        {
            writeLock.unlock();
        }
    }

    private void onClusterChanged(ClusterSnapshot s)
    {
        ring.updateClusterState(s);
        dispatchUndelivered(s);
    }

    private ClusterSnapshot getClusterSnapshot() {
        readLock.lock();
        try {
            return new ClusterSnapshot(ipToUUID, ipToTokens.asMap(), tokenToIp);
        } finally {
            readLock.unlock();
        }
    }

    private synchronized void maybeDispatch(LegacyEvent event)
    {
        ClusterSnapshot snapshot = getClusterSnapshot();

        if (!snapshot.containsEndpointIds(event.requiredEndpoints()) ||
            !snapshot.containsTokensFromEndpoints(event.requiredEndpoints()))
        {
            undelivered.add(event);
            return;
        }

        doDispatch(snapshot, event);
    }

    private synchronized void dispatchUndelivered(ClusterSnapshot snapshot)
    {
        Set<LegacyEvent> undeliveredSnapshot = new HashSet<>(undelivered);

        Set<LegacyEvent> dispatched = new HashSet<>();

        for (LegacyEvent event : undeliveredSnapshot)
        {
            if (snapshot.containsEndpointIds(event.requiredEndpoints()) &&
                snapshot.containsTokensFromEndpoints(event.requiredEndpoints()))
            {
                doDispatch(snapshot, event);
                dispatched.add(event);
            }
        }

        if (!dispatched.isEmpty())
            undelivered.removeAll(dispatched);
    }

    private void doDispatch(ClusterSnapshot snapshot, LegacyEvent event)
    {
        event.getTokenEvents(snapshot).forEach(e -> ring.handleEvent(e));
    }

    private Optional<ClusterSnapshot> updateTokens(InetAddressAndPort endpoint, VersionedValue value)
    {
        Set<Token> newTokens = null;
        try
        {
            newTokens = TokenSerializer.deserializeAsSet(partitioner, new DataInputStream(new ByteArrayInputStream(value.toBytes())));
        }
        catch (IOException e)
        {
            logger.warn("Problem while deserializing tokens from node {}.", endpoint, e);
            return Optional.empty();
        }

        assert newTokens != null : "newTokens should be initialized";

        readLock.lock();
        try
        {
            Collection<Token> oldTokens = ipToTokens.get(endpoint);
            if (newTokens.equals(new HashSet<>(oldTokens)))
                return Optional.empty();
        }
        finally
        {
            readLock.unlock();
        }

        writeLock.lock();
        try
        {
            Collection<Token> oldTokens = ipToTokens.removeAll(endpoint);
            if (oldTokens.isEmpty())
            {
                logger.debug("Node {} tokens are {}.", endpoint, newTokens);
            }
            else
            {
                logger.warn("Node {} changing tokens from {} to {}.", oldTokens, newTokens);
                oldTokens.forEach(t -> tokenToIp.remove(t));
            }
            newTokens.forEach(t ->
                              {
                                  ipToTokens.put(endpoint, t);
                                  tokenToIp.put(t, endpoint);
                              });
            return Optional.of(getClusterSnapshot());
        }
        finally
        {
            writeLock.unlock();
        }
    }

    private Optional<ClusterSnapshot> updateHostId(InetAddressAndPort endpoint, VersionedValue value)
    {
        UUID newId = UUID.fromString(value.value);

        readLock.lock();
        try
        {
            if (newId.equals(ipToUUID.get(endpoint)))
                return Optional.empty();
        }
        finally
        {
            readLock.unlock();
        }

        writeLock.lock();
        try
        {
            UUID prevId = ipToUUID.put(endpoint, newId);
            if (prevId == null)
            {
                logger.debug("Node {} id is {}.", endpoint, newId);
            }
            else
            {
                logger.warn("Node {} ({}) previously associated with id {}.", newId, endpoint, prevId);
            }
            return Optional.of(getClusterSnapshot());
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /* NO-OP */

    public void onJoin(InetAddressAndPort endpoint, EndpointState epState)
    {

    }

    public void beforeChange(InetAddressAndPort endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
    {

    }

    public void onAlive(InetAddressAndPort endpoint, EndpointState state)
    {

    }

    public void onDead(InetAddressAndPort endpoint, EndpointState state)
    {

    }

    public void onRemove(InetAddressAndPort endpoint)
    {

    }

    public void onRestart(InetAddressAndPort endpoint, EndpointState state)
    {

    }
}
