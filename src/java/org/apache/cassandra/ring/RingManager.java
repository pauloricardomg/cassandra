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

package org.apache.cassandra.ring;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.ring.node.NodeState;
import org.apache.cassandra.ring.token.VirtualNode;

public class RingManager implements IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(RingManager.class);

    private final IPartitioner partitioner;
    private final Function<InetAddressAndPort, NodeInfo> getNodeInfo;

    private final AtomicReference<RingSnapshot> ringState = new AtomicReference<>(new RingSnapshot());

    public RingManager(IPartitioner partitioner, Function<InetAddressAndPort, NodeInfo> getNodeInfo) {
        this.partitioner = partitioner;
        this.getNodeInfo = getNodeInfo;
    }

    public synchronized void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue nodeStatus)
    {
        if (state != ApplicationState.STATUS && state != ApplicationState.STATUS_WITH_PORT)
            return;

        NodeInfo nodeInfo = getNodeInfo.apply(endpoint);

        assert nodeInfo != null : String.format("Information missing for host %s.", endpoint);

        NodeState nodeState = NodeState.create(nodeStatus, partitioner, nodeInfo.tokens, getNodeInfo);
        List<VirtualNode> newVirtualNodes = nodeInfo.tokens.stream().flatMap(t -> nodeState.mapToTokenStates(t, nodeInfo.dc, nodeInfo.rack, nodeInfo.id).stream())
                                                           .collect(Collectors.toList());

        RingSnapshot newRing = ringState.get().withAppliedStates(newVirtualNodes);
        maybeUpdateRingState(newRing);
    }

    public synchronized void onDead(InetAddressAndPort endpoint, EndpointState state)
    {
        UUID hostId = state.getHostId();
        assert hostId != null : String.format("Host id is missing for dead host %s", endpoint);
        maybeUpdateRingState(ringState.get().withDownHost(hostId));
    }

    public synchronized void onRemove(InetAddressAndPort endpoint)
    {
        NodeInfo nodeInfo = getNodeInfo.apply(endpoint);
        assert nodeInfo != null : String.format("Information missing for dead host %s.", endpoint);
        maybeUpdateRingState(ringState.get().withRemovedHost(nodeInfo.id));
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

    public RingSnapshot getRingSnapshot()
    {
        return ringState.get();
    }
}
