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

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.state.ClusterSnapshot;
import org.apache.cassandra.state.token.ReplaceTokenEvent;
import org.apache.cassandra.state.token.TokenEvent;

public class ReplaceEndpointEvent extends LegacyEvent
{
    final InetAddressAndPort oldNode;
    final InetAddressAndPort newNode;

    public ReplaceEndpointEvent(InetAddressAndPort oldNode, InetAddressAndPort newNode)
    {
        this.oldNode = oldNode;
        this.newNode = newNode;
    }

    public Collection<InetAddressAndPort> requiredEndpoints()
    {
        return ImmutableList.of(oldNode, newNode);
    }

    public List<TokenEvent> getTokenEvents(ClusterSnapshot snapshot)
    {
        UUID newNodeId = snapshot.getId(newNode);
        return snapshot.getTokens(oldNode).stream().map(t -> new ReplaceTokenEvent(t, newNodeId)).collect(Collectors.toList());
    }
}