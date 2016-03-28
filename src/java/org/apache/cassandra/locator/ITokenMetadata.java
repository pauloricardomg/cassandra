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

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.BiMultiValMap;
import org.apache.cassandra.utils.Pair;

/**
 * Created by paulo on 3/27/16.
 */
public interface ITokenMetadata
{
    @VisibleForTesting
    ITokenMetadata cloneWithNewPartitioner(IPartitioner newPartitioner);

    int pendingRangeChanges(InetAddress source);

    void updateNormalToken(Token token, InetAddress endpoint);

    void updateNormalTokens(Collection<Token> tokens, InetAddress endpoint);

    void updateNormalTokens(Multimap<InetAddress, Token> endpointTokens);

    void updateHostId(UUID hostId, InetAddress endpoint);

    UUID getHostId(InetAddress endpoint);

    InetAddress getEndpointForHostId(UUID hostId);

    Map<InetAddress, UUID> getEndpointToHostIdMapForReading();

    @Deprecated
    void addBootstrapToken(Token token, InetAddress endpoint);

    void addBootstrapTokens(Collection<Token> tokens, InetAddress endpoint);

    @Deprecated
    void removeBootstrapTokens(Collection<Token> tokens);

    void addLeavingEndpoint(InetAddress endpoint);

    void addMovingEndpoint(Token token, InetAddress endpoint);

    void removeEndpoint(InetAddress endpoint);

    void updateTopology(InetAddress endpoint);

    void updateTopology();

    void removeFromMoving(InetAddress endpoint);

    Collection<Token> getTokens(InetAddress endpoint);

    @Deprecated
    Token getToken(InetAddress endpoint);

    boolean isMember(InetAddress endpoint);

    boolean isLeaving(InetAddress endpoint);

    boolean isMoving(InetAddress endpoint);

    ITokenMetadata cloneOnlyTokenMap();

    ITokenMetadata cachedOnlyTokenMap();

    ITokenMetadata cloneAfterAllLeft();

    ITokenMetadata cloneAfterAllSettled();

    InetAddress getEndpoint(Token token);

    Collection<Range<Token>> getPrimaryRangesFor(Collection<Token> tokens);

    @Deprecated
    Range<Token> getPrimaryRangeFor(Token right);

    List<Token> sortedTokens();

    Multimap<Range<Token>, InetAddress> getPendingRangesMM(String keyspaceName);

    PendingRangeMaps getPendingRanges(String keyspaceName);

    List<Range<Token>> getPendingRanges(String keyspaceName, InetAddress endpoint);

    void calculatePendingRanges(AbstractReplicationStrategy strategy, String keyspaceName);

    Token getPredecessor(Token token);

    Set<InetAddress> getBootstrappingEndpoints();

    Set<InetAddress> getAllEndpoints();

    Set<InetAddress> getLeavingEndpoints();

    Set<InetAddress> getMovingEndpoints();

    void clearUnsafe();

    String toString();

    Collection<InetAddress> pendingEndpointsFor(Token token, String keyspaceName);

    Collection<InetAddress> getWriteEndpoints(Token token, String keyspaceName, Collection<InetAddress> naturalEndpoints);

    Multimap<InetAddress, Token> getEndpointToTokenMapForReading();

    Map<Token, InetAddress> getNormalAndBootstrappingTokenToEndpointMap();

    TokenMetadata.Topology getTopology();

    long getRingVersion();

    void invalidateCachedRings();

    DecoratedKey decorateKey(ByteBuffer key);
}
