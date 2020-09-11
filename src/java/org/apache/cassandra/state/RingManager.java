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
import java.util.List;
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
import org.apache.cassandra.state.node.NodeState;
import org.apache.cassandra.state.token.TokenState;

public class RingManager implements IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(RingManager.class);

    private final IPartitioner partitioner;
    private final Function<InetAddressAndPort, Collection<Token>> tokenGetter;
    private final Function<InetAddressAndPort, UUID> idGetter;

    public final AtomicReference<RingSnapshot> ringState = new AtomicReference<>(new RingSnapshot());

    public RingManager(IPartitioner partitioner, Function<InetAddressAndPort, Collection<Token>> tokenGetter,
                       Function<InetAddressAndPort, UUID> idGetter) {
        this.partitioner = partitioner;
        this.tokenGetter = tokenGetter;
        this.idGetter = idGetter;
    }

    public synchronized void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
    {
        if (state != ApplicationState.STATUS && state != ApplicationState.STATUS_WITH_PORT)
            return;

        UUID id = idGetter.apply(endpoint);
        Collection<Token> tokens = tokenGetter.apply(endpoint);

        assert id != null && tokens != null && !tokens.isEmpty() : String.format("Id (%s) or tokens (%s) missing for endpoint %s.", id, tokens, endpoint);

        NodeState nodeState = NodeState.extract(value, partitioner, id, tokens, idGetter);

        List<TokenState> newTokenStates = tokens.stream().flatMap(t -> nodeState.mapToTokenStates(id, t).stream()).collect(Collectors.toList());
        RingSnapshot newRing = ringState.get().withAppliedStates(newTokenStates);

        maybeUpdateRingState(newRing);
    }

    public synchronized void onDead(InetAddressAndPort endpoint, EndpointState state)
    {
        UUID hostId = state.getHostId();
        assert hostId != null : String.format("Host id is missing for dead host %s");
        maybeUpdateRingState(ringState.get().withDownHost(hostId));
    }

    public synchronized void onRemove(InetAddressAndPort endpoint)
    {
        UUID hostId = idGetter.apply(endpoint);
        assert hostId != null : String.format("Host id is missing for dead host %s");
        maybeUpdateRingState(ringState.get().withRemovedHost(hostId));
    }

    private void maybeUpdateRingState(RingSnapshot newRing)
    {
        if (ringState.get().version != newRing.version)
        {
            logger.debug("Changing ring state from version {} to version {}.", ringState.get().version, newRing.version);
            ringState.set(newRing);
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

    public void onRestart(InetAddressAndPort endpoint, EndpointState state)
    {

    }
}
