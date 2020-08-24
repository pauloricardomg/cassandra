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

package org.apache.cassandra.state.legacy;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.state.ClusterSnapshot;
import org.apache.cassandra.state.token.TokenEvent;

public abstract class LegacyEvent
{
    public Collection<InetAddressAndPort> requiredEndpoints()
    {
        return Collections.emptyList();
    }

    public List<TokenEvent> getTokenEvents(ClusterSnapshot snapshot)
    {
        return Collections.emptyList();
    }

    public static LegacyEvent get(InetAddressAndPort endpoint, VersionedValue value, IPartitioner partitioner)
    {
        String[] pieces = value.value.split(VersionedValue.DELIMITER_STR, -1);
        String moveName = pieces[0];
        switch (moveName)
        {
            case VersionedValue.STATUS_BOOTSTRAPPING_REPLACE:
                try
                {
                    InetAddressAndPort oldNode = InetAddressAndPort.getByName(pieces[1]);
                    return new ReplaceEndpointEvent(oldNode, endpoint);
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(String.format("Node %s tried to replace malformed endpoint %s.", endpoint, pieces[1]), e);
                }

            case VersionedValue.STATUS_BOOTSTRAPPING:
                return new BootstrapEndpointEvent(endpoint);

            case VersionedValue.STATUS_NORMAL:
                return new NormalEndpointEvent(endpoint);

            case VersionedValue.SHUTDOWN:
                return new ShutdownEndpointEvent(endpoint);

            case VersionedValue.REMOVING_TOKEN:
                return new RemovingEndpointEvent(endpoint);

            case VersionedValue.REMOVED_TOKEN:
            case VersionedValue.STATUS_LEFT:
                long expireTime = Long.parseLong(pieces[2]);
                return new RemovedEndpointEvent(endpoint, expireTime);

            case VersionedValue.STATUS_LEAVING:
                return new LeavingEndpointEvent(endpoint);

            case VersionedValue.STATUS_MOVING:
                Token token = partitioner.getTokenFactory().fromString(pieces[1]);
                return new MoveEndpointEvent(endpoint, token);

            default:
                throw new IllegalStateException();
        }
    }
}