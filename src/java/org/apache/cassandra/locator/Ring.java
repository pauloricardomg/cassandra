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

package org.apache.cassandra.locator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Token;

public class Ring
{
    private static final Logger logger = LoggerFactory.getLogger(Ring.class);

    public static final class OwnershipInfo
    {
        public final UUID owner;
        private boolean isAdding = false;
        private boolean isRemoving = false;

        public OwnershipInfo(UUID ownerId)
        {
            this.owner = ownerId;
        }

        public boolean isPending()
        {
            return isAdding || isRemoving;
        }

        public boolean isRemoving()
        {
            return isRemoving;
        }

        public void setIsRemoving(boolean isRemoving)
        {
            this.isRemoving = isRemoving;
        }

        public boolean isAdding()
        {
            return isAdding;
        }

        public void setIsAdding(boolean isAdding)
        {
            this.isAdding = isAdding;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            OwnershipInfo that = (OwnershipInfo) o;

            if (isAdding != that.isAdding) return false;
            if (isRemoving != that.isRemoving) return false;
            return owner.equals(that.owner);
        }

        public int hashCode()
        {
            int result = owner.hashCode();
            result = 31 * result + (isAdding ? 1 : 0);
            result = 31 * result + (isRemoving ? 1 : 0);
            return result;
        }
    }

    final NavigableMap<Token, Map<UUID, OwnershipInfo>> ring;
    final Multimap<UUID, Token> reverseRing;

    public Ring()
    {
        this(new TreeMap<>());
    }

    public Ring(NavigableMap<Token, Map<UUID, OwnershipInfo>> ring)
    {
        this.ring = ring;
        this.reverseRing = HashMultimap.create();
        for (Map.Entry<Token, Map<UUID, OwnershipInfo>> entry : ring.entrySet())
        {
            for (OwnershipInfo ownershipInfo : entry.getValue().values())
            {
                this.reverseRing.put(ownershipInfo.owner, entry.getKey());
            }
        }
    }

    public void setNormal(UUID nodeId, Token token)
    {
        Optional<OwnershipInfo> optInfo = getOwnershipInfo(token, nodeId);
        if (optInfo.isPresent())
        {
            OwnershipInfo info = optInfo.get();
            assert !(info.isAdding && info.isRemoving);
            if (info.isAdding)
            {
                logger.info("Node {} finished adding token {}.", nodeId, token);
                info.setIsAdding(false);
            }
            if (info.isRemoving)
            {
                logger.info("Node {} cancelled removal of token {}.", nodeId, token);
                info.setIsRemoving(false);
            }
        }
        else
        {
            OwnershipInfo info = new OwnershipInfo(nodeId);
            addOwnershipInfo(token, info);
        }
    }

    public void setAdding(UUID nodeId, Token token)
    {
        Optional<OwnershipInfo> optInfo = getOwnershipInfo(token, nodeId);
        if (optInfo.isPresent())
        {
            logger.warn("Node {} already has token {}. Setting state to adding.", nodeId, token);
            optInfo.get().setIsAdding(true);
            return;
        }
        OwnershipInfo info = new OwnershipInfo(nodeId);
        info.setIsAdding(true);
        addOwnershipInfo(token, info);
    }

    public void setRemoving(UUID nodeId, Token token)
    {
        Optional<OwnershipInfo> optInfo = getOwnershipInfo(token, nodeId);
        if (!optInfo.isPresent())
        {
            logger.warn("Node {} does not own token {}.", nodeId, token);
            return;
        }
        optInfo.get().setIsRemoving(true);
    }

    public void setRemoving(UUID nodeId)
    {
        Collection<Token> tokens = reverseRing.get(nodeId);
        for (Token token : tokens)
        {
            Map<UUID, OwnershipInfo> ownershipInfos = ring.get(token);
            assert ownershipInfos != null && ownershipInfos.containsKey(nodeId);
            ownershipInfos.get(nodeId).setIsRemoving(true);
        }
    }

    public void removeNode(UUID nodeId)
    {
        Collection<Token> tokens = reverseRing.get(nodeId);
        for (Token token : tokens)
        {
            removeToken(nodeId, token);
        }
    }

    private void removeToken(UUID nodeId, Token token)
    {
        Map<UUID, OwnershipInfo> ownershipInfos = ring.get(token);
        if (ownershipInfos != null)
        {
            OwnershipInfo removed = ownershipInfos.remove(nodeId);
            if (removed.isPending())
                logger.warn("Removing non-pending token {} from node {}.", nodeId, token);
        }
        if (ownershipInfos.isEmpty())
            ring.remove(token);
        reverseRing.remove(nodeId, token);
    }

    private Optional<OwnershipInfo> addOwnershipInfo(Token token, OwnershipInfo info)
    {
        Map<UUID, OwnershipInfo> ownershipInfos = ring.get(token);
        if (ownershipInfos == null)
        {
            ownershipInfos = new HashMap<>(1);
            ring.put(token, ownershipInfos);
        }

        reverseRing.put(info.owner, token);

        return Optional.of(ownershipInfos.put(info.owner, info));
    }

    private Optional<OwnershipInfo> getOwnershipInfo(Token token, UUID ownerId)
    {
        Map<UUID, OwnershipInfo> ownershipInfos = ring.get(token);
        if (ownershipInfos == null)
            return Optional.empty();
        return Optional.of(ownershipInfos.get(ownerId));
    }

    public Collection<Token> getTokens(UUID nodeId)
    {
        return reverseRing.get(nodeId);
    }

    public Collection<Token> removePendingTokens(UUID nodeId)
    {
        Collection<Token> tokens = reverseRing.get(nodeId);
        for (Token token : tokens)
        {
            Optional<OwnershipInfo> optInfo = getOwnershipInfo(token, nodeId);
            if (optInfo.isPresent() && optInfo.get().isRemoving)
                removeToken(nodeId, token);
        }
        return tokens;
    }

    private boolean hasToken(UUID nodeId, Predicate<OwnershipInfo> predicate)
    {
        Collection<Token> tokens = reverseRing.get(nodeId);
        if (tokens != null)
        {
            for (Token token : reverseRing.get(nodeId))
            {
                Optional<OwnershipInfo> ownershipInfo = getOwnershipInfo(token, nodeId);
                if (predicate.test(ownershipInfo.get()))
                    return true;
            }
        }
        return false;
    }

    public boolean hasNormalToken(UUID nodeId)
    {
        return hasToken(nodeId, i -> !i.isPending());
    }

    public boolean isRemovingToken(UUID nodeId)
    {
        return hasToken(nodeId, i -> i.isRemoving);
    }

    public boolean isAddingToken(UUID nodeId)
    {
        return hasToken(nodeId, i -> i.isAdding);
    }

    public Token getPredecessor(Token token)
    {
        return getPredecessor(token, Ring::hasNormalToken);
    }

    private Token getPredecessor(Token token, Predicate<Collection<OwnershipInfo>> predicate)
    {
        Map.Entry<Token, Map<UUID, OwnershipInfo>> current = ring.floorEntry(token);
        while (current != null && predicate.test(current.getValue().values()))
        {
            current = ring.lowerEntry(current.getKey());
        }
        if (current == null)
            return ring.lastKey();
        return current.getKey();
    }

    private Stream<Map.Entry<Token, Map<UUID, OwnershipInfo>>> getTokens(Predicate<Collection<OwnershipInfo>> predicate)
    {
        return ring.entrySet().stream().filter(e -> predicate.test(e.getValue().values()));
    }

    public List<Token> getNonPendingTokens()
    {
        return getTokens(Ring::hasNormalToken).map(e -> e.getKey()).collect(Collectors.toList());
    }

    public Set<UUID> getNonPendingOwners(Token token)
    {
        Map<UUID, OwnershipInfo> ownershipMap = ring.get(token);
        if (ownershipMap != null)
            return ownershipMap.entrySet().stream().filter(e -> !e.getValue().isPending()).map(e -> e.getKey()).collect(Collectors.toSet());
        return Collections.emptySet();
    }

    public Map<Token, Set<UUID>> getAddingTokens()
    {
        HashMap<Token, Set<UUID>> result = new HashMap<>();
        getTokens(Ring::isAddingToken).forEach(e -> result.put(e.getKey(), e.getValue().keySet()));
        return result;
    }

    public Map<Token, Set<UUID>> getRemovingTokens()
    {
        HashMap<Token, Set<UUID>> result = new HashMap<>();
        getTokens(Ring::isAddingToken).forEach(e -> result.put(e.getKey(), e.getValue().keySet()));
        return result;
    }

    private static boolean hasNormalToken(Collection<OwnershipInfo> info)
    {
        return info.stream().anyMatch(i -> !i.isPending());
    }

    private static boolean isAddingToken(Collection<OwnershipInfo> info)
    {
        return info.stream().anyMatch(i -> i.isAdding);
    }

    private static boolean isRemovingToken(Collection<OwnershipInfo> info)
    {
        return info.stream().anyMatch(i -> i.isRemoving);
    }

    public void clear()
    {
        ring.clear();
        reverseRing.clear();
    }
}
