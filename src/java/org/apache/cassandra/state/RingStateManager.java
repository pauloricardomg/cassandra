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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.state.legacy.LegacyState;

public class RingStateManager implements IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(RingStateManager.class);

    private final IPartitioner partitioner;
    private final Function<InetAddressAndPort, Collection<Token>> tokenGetter;
    private final Function<InetAddressAndPort, UUID> idGetter;

    public final AtomicReference<RingState> ringState = new AtomicReference<>(new RingState());
    private final Map<InetAddressAndPort, Queue<VersionedValue>> undelivered = new HashMap<>();

    public RingStateManager(IPartitioner partitioner, Function<InetAddressAndPort, Collection<Token>> tokenGetter,
                            Function<InetAddressAndPort, UUID> idGetter) {
        this.partitioner = partitioner;
        this.tokenGetter = tokenGetter;
        this.idGetter = idGetter;
    }

    public synchronized void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
    {
        switch (state) {
            case HOST_ID:
                UUID newId = UUID.fromString(value.value);
                RingState newRing = ringState.get().applyNodeState(NodeState.create(newId, endpoint));
                maybeUpdateRingState(newRing);
                maybeDispatchUndelivered(endpoint);
                break;

            case TOKENS:
                maybeDispatchUndelivered(endpoint);

            case STATUS:
            case STATUS_WITH_PORT:
                maybeDispatch(endpoint, value);
                break;
        }
    }

    private synchronized void maybeUpdateRingState(RingState newRing)
    {
        if (ringState.get().version != newRing.version)
        {
            logger.info("Changing ring state from version {} to version {}.", ringState.get().version, newRing.version);
            ringState.set(newRing);
        }
    }

    private synchronized void maybeDispatch(InetAddressAndPort endpoint, VersionedValue nodeStatus)
    {
        UUID id = idGetter.apply(endpoint);
        Collection<Token> tokens = tokenGetter.apply(endpoint);
        if (id == null || tokens.isEmpty())
        {
            logger.debug("Tokens ({}) or id ({}) missing for endpoint {}. Will queue status {} for delivery later.", tokens, id, endpoint, nodeStatus);
            undelivered.compute(endpoint, (k, v) -> v == null ? new LinkedList<>() : v).add(nodeStatus);
            return;
        }

        doDispatch(id, tokens, nodeStatus);
    }

    private synchronized void maybeDispatchUndelivered(InetAddressAndPort endpoint)
    {
        Queue<VersionedValue> toDeliver = undelivered.get(endpoint);
        if (toDeliver == null)
            return;

        UUID id = idGetter.apply(endpoint);
        Collection<Token> tokens = tokenGetter.apply(endpoint);
        if (id == null || tokens.isEmpty())
        {
            logger.debug("Tokens ({}) or id ({}) missing for endpoint {}. Cannot deliver undelivered status updates: {}.", tokens, id, endpoint, undelivered);
            return;
        }

        for (VersionedValue nodeStatus : toDeliver)
        {
            doDispatch(id, tokens, nodeStatus);
        }

        undelivered.remove(endpoint);
    }

    private synchronized void doDispatch(UUID id, Collection<Token> tokens, VersionedValue nodeStatus)
    {
        LegacyState legacyState = LegacyState.extract(nodeStatus, partitioner, id, tokens, idGetter);
        List<TokenState> newStates = tokens.stream().flatMap(t -> legacyState.mapToTokenStates(id, t, idGetter).stream()).collect(Collectors.toList());
        RingState newRing = ringState.get().applyTokenStates(newStates);
        maybeUpdateRingState(newRing);
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
