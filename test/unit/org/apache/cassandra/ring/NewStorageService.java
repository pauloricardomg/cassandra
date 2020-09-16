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

import java.util.Map;
import java.util.function.Function;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.locator.TokenMetadata;

public class NewStorageService implements FakeStorageService
{
    final RingManager ringManager;
    final NetworkTopologyStrategy nts;

    public NewStorageService(Map<String, String> dcRfs, Function<InetAddressAndPort, NodeInfo> nodeInfoGetter)
    {
        this.ringManager = new RingManager(DatabaseDescriptor.getPartitioner(), nodeInfoGetter);
        this.nts = new NetworkTopologyStrategy("dummy", new TokenMetadata(), new SimpleSnitch(), dcRfs);
    }

    public RingOverlay getRing()
    {
        return nts.createReplicatedRing(ringManager.getRingSnapshot());
    }

    public synchronized void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue nodeStatus)
    {
        ringManager.onChange(endpoint, state, nodeStatus);
    }

    public synchronized void onDead(InetAddressAndPort endpoint, EndpointState state)
    {
        ringManager.onDead(endpoint, state);
    }

    public synchronized void onRemove(InetAddressAndPort endpoint)
    {
        ringManager.onRemove(endpoint);
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
