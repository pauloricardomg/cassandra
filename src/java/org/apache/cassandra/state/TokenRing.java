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

import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.state.token.TokenEvent;
import org.apache.cassandra.state.token.TokenState;

public class TokenRing
{
    private volatile long version = 0;
    private volatile ClusterSnapshot cluster = new ClusterSnapshot();

    private final TreeMap<Token, TokenState> tokens = new TreeMap<>();

    public synchronized void updateClusterState(ClusterSnapshot snapshot)
    {
        version++;
        cluster = snapshot;
    }

    public synchronized void handleEvent(TokenEvent e)
    {
        tokens.compute(e.getToken(), (token, previousState) ->
        {
            TokenState newState = e.computeNewState(previousState);
            if (newState != previousState)
                version++;
            return newState;
        });
    }
}
