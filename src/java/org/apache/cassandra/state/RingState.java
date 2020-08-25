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
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;

public class RingState
{
    static Logger logger = LoggerFactory.getLogger(RingState.class);

    final long version;
    private final SortedMap<Token, TokenState> tokens;
    private final Map<UUID, InetAddressAndPort> nodes;

    public RingState()
    {
        this(0, Collections.emptySortedMap(), Collections.emptyMap());
    }

    private RingState(long version, SortedMap<Token, TokenState> tokens, Map<UUID, InetAddressAndPort> nodes)
    {
        this.version = version;
        this.tokens = tokens;
        this.nodes = nodes;
    }

    public RingState applyTokenStates(Collection<TokenState> newStates)
    {
        TreeMap<Token, TokenState> newTokens = new TreeMap<>(tokens);
        AtomicBoolean updatedState = new AtomicBoolean(false);

        newStates.forEach(s ->
                          tokens.compute(s.token, (token, previousState) ->
                          {
                              if (!s.equals(previousState))
                              {
                                  validateStateChange(previousState, s);
                                  updatedState.set(true);
                              }
                              return s;
                          }));

        if (!updatedState.get())
            return this;

        return new RingState(version + 1, newTokens, nodes);
    }

    private void validateStateChange(TokenState previousState, TokenState newState)
    {

    }

    public RingState applyNodeState(NodeState newState)
    {
        if (newState.ip.equals(nodes.get(newState.id)))
            return this;

        ImmutableMap.Builder<UUID, InetAddressAndPort> nodesBuilder  = ImmutableMap.builder();
        nodesBuilder.putAll(nodes);
        nodesBuilder.put(newState.id, newState.ip);

        return new RingState(version + 1, tokens, nodesBuilder.build());
    }
}
