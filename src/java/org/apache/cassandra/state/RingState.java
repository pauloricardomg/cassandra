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
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.state.token.MovingToState;
import org.apache.cassandra.state.token.TokenState;

public class RingState
{
    static Logger logger = LoggerFactory.getLogger(RingState.class);

    final long version;
    private final SortedMap<Token, TokenState> tokens;

    public RingState()
    {
        this(0, Collections.emptySortedMap());
    }

    private RingState(long version, SortedMap<Token, TokenState> tokens)
    {
        this.version = version;
        this.tokens = tokens;
    }

    public RingState applyTokenStates(Collection<TokenState> newStates)
    {
        TreeMap<Token, TokenState> newTokens = new TreeMap<>(tokens);
        AtomicBoolean updatedState = new AtomicBoolean(false);

        newStates.forEach(newState ->
                          {
                              if (applyNewState(newTokens, newState))
                                  updatedState.set(true);
                          });

        if (!updatedState.get())
            return this;

        return new RingState(version + 1, newTokens);
    }

    private static boolean applyNewState(TreeMap<Token, TokenState> tokenMap, TokenState newState)
    {
        TokenState oldState = newState.isRemoved() ?  removeToken(tokenMap, newState.token) : tokenMap.put(newState.token, newState);
        if (oldState == null)
            oldState = TokenState.initial(newState.token, newState.owner);

        assert oldState.canTransitionTo(newState) && newState.canTransitionFrom(oldState) : String.format("Cannot transition token %s state from %s to %s.", newState.token, oldState, newState);

        logger.debug("Transitioning token %s from state %s to state %s.", newState.token, oldState, newState);

        if (oldState.isMovingTo())
        {
            MovingToState movingToState = (MovingToState) oldState;
            TokenState movingFromState = removeToken(tokenMap, movingToState.oldToken);
            assert movingFromState != null && movingFromState.canMoveTo(newState) : String.format("Cannot move token %s state %s to token % state %s.", movingToState.oldToken, movingFromState, newState.token, newState);
            logger.debug("Removing token %s from owner %s that moved to %s.", movingToState.oldToken, movingFromState.owner, newState.token);

        }

        return !newState.equals(oldState);
    }

    private static TokenState removeToken(TreeMap<Token, TokenState> tokenMap, Token token)
    {
        return tokenMap.remove(token);
    }
}
