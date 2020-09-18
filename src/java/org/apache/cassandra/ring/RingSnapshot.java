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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.ring.token.MovingToState;
import org.apache.cassandra.ring.token.VirtualNode;

public class RingSnapshot
{
    static Logger logger = LoggerFactory.getLogger(RingSnapshot.class);

    final long version;
    private final SortedMap<Token, VirtualNode> vnodes;

    public RingSnapshot()
    {
        this(0, Collections.emptySortedMap());
    }

    private RingSnapshot(long version, SortedMap<Token, VirtualNode> vnodes)
    {
        this.version = version;
        this.vnodes = vnodes;
    }

    public RingSnapshot withRemovedHost(UUID hostId)
    {
        List<VirtualNode> removedStates = vnodes.values().stream().filter(t -> hostId.equals(t.owner)).map(t -> VirtualNode.removed(t.token, t.dc, t.rack, t.owner)).collect(Collectors.toList());
        return withAppliedStates(removedStates);
    }

    public RingSnapshot withDownHost(UUID hostId)
    {
        List<VirtualNode> abortedStates = vnodes.values().stream().filter(t -> hostId.equals(t.owner)).map(t -> t.maybeAbort()).collect(Collectors.toList());
        return withAppliedStates(abortedStates);
    }

    public RingSnapshot withAppliedStates(Collection<VirtualNode> diff)
    {
        TreeMap<Token, VirtualNode> newTokens = new TreeMap<>(vnodes);
        AtomicBoolean updatedState = new AtomicBoolean(false);

        diff.forEach(newState ->
                          {
                              if (applyNewState(newTokens, newState))
                                  updatedState.set(true);
                          });

        if (!updatedState.get())
            return this;

        return new RingSnapshot(version + 1, newTokens);
    }

    private static boolean applyNewState(TreeMap<Token, VirtualNode> tokenMap, VirtualNode newState)
    {
        VirtualNode oldState = newState.isRemoved() ? removeToken(tokenMap, newState.token) : tokenMap.put(newState.token, newState);
        if (oldState == null)
            oldState = VirtualNode.initial(newState.token, newState.dc, newState.rack, newState.owner);

        if (oldState.equals(newState))
            return false;

        assert oldState.canTransitionTo(newState) && newState.canTransitionFrom(oldState) : String.format("Cannot transition token %s state from %s to %s.", newState.token, oldState, newState);

        logger.debug("Transitioning token {} from state {} to state {}.", newState.token, oldState, newState);

        if (oldState.isMovingTo())
        {
            MovingToState movingToState = (MovingToState) oldState;
            VirtualNode movingFromState = removeToken(tokenMap, movingToState.oldToken);
            assert movingFromState != null && movingFromState.canMoveTo(newState) : String.format("Cannot move token %s (state %s) to token % (state %s).", movingToState.oldToken, movingFromState, newState.token, newState);
            logger.debug("Removing token {} from owner {} that moved to {}.", movingToState.oldToken, movingFromState.owner, newState.token);
        }

        return true;
    }

    private static VirtualNode removeToken(TreeMap<Token, VirtualNode> tokenMap, Token token)
    {
        return tokenMap.remove(token);
    }

    public RingIterator iterator()
    {
        return new RingIterator(new ArrayList<>(vnodes.values()));
    }

    public RingIterator iterator(String dcName)
    {
        return new RingIterator(vnodes.values().stream().filter(t -> dcName.equals(t.dc)).collect(Collectors.toCollection(ArrayList::new)));
    }

    public int getRackCount(String dcName)
    {
        return (int) vnodes.values().stream().filter(t -> dcName.equals(t.dc)).map(t -> t.rack).distinct().count();
    }

    public boolean hasPendingNodes()
    {
        return vnodes.values().stream().anyMatch(v -> v.isPending());
    }
}
