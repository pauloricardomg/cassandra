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
import java.util.UUID;
import java.util.function.Function;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.state.TokenState;

public class LegacyState
{
    enum Status
    {
        BOOTSTRAPPING_REPLACE,
        BOOTSTRAPPING,
        NORMAL,
        SHUTDOWN,
        REMOVING_TOKEN,
        REMOVED_TOKEN,
        LEFT,
        LEAVING,
        MOVING;
    }

    final Status status;

    protected LegacyState(Status status)
    {
        this.status = status;
    }

    public Collection<TokenState> mapToTokenStates(UUID id, Token token, Function<InetAddressAndPort, UUID> idGetter)
    {
        switch (status)
        {
            case BOOTSTRAPPING_REPLACE:
                return Collections.singleton(TokenState.replacing(token, oldId, id));

            case BOOTSTRAPPING:
                return Collections.singleton(TokenState.adding(token, id));

            case NORMAL:
                return Collections.singleton(TokenState.normal(token, id));

            case LEAVING:
            case REMOVING_TOKEN:
                return Collections.singleton(TokenState.removing(token, id));

            case LEFT:
            case REMOVED_TOKEN:
                return Collections.singleton(TokenState.removed(token, id));

            default:
                // Must be overriden by subclasses
                throw new UnsupportedOperationException();
        }
    }

    public static LegacyState extract(VersionedValue value, IPartitioner partitioner, UUID nodeId, Collection<Token> tokens,
                                      Function<InetAddressAndPort, UUID> idGetter)
    {
        String[] pieces = value.value.split(VersionedValue.DELIMITER_STR, -1);
        String moveName = pieces[0];
        switch (moveName)
        {
            case VersionedValue.STATUS_BOOTSTRAPPING_REPLACE:
                try
                {
                    InetAddressAndPort originalNode = InetAddressAndPort.getByName(pieces[1]);
                    UUID originalNodeId = idGetter.apply(originalNode);
                    return new BootReplaceState(originalNodeId, nodeId);
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(String.format("Node tried to replace malformed endpoint %s.", pieces[1]), e);
                }

            case VersionedValue.STATUS_BOOTSTRAPPING:
                return new LegacyState(Status.BOOTSTRAPPING);

            case VersionedValue.STATUS_NORMAL:
                return new LegacyState(Status.NORMAL);

            case VersionedValue.SHUTDOWN:
                return new LegacyState(Status.SHUTDOWN);

            case VersionedValue.REMOVING_TOKEN:
                return new LegacyState(Status.REMOVING_TOKEN);

            case VersionedValue.REMOVED_TOKEN:
                return new LegacyState(Status.REMOVED_TOKEN);

            case VersionedValue.STATUS_LEFT:
                return new LegacyState(Status.LEFT);

            case VersionedValue.STATUS_LEAVING:
                return new LegacyState(Status.LEAVING);

            case VersionedValue.STATUS_MOVING:
                assert tokens.size() == 1 : "Node should have only one token";
                Token oldToken = tokens.iterator().next();
                Token newToken = partitioner.getTokenFactory().fromString(pieces[1]);
                return new MovingLegacyState(oldToken, newToken);

            default:
                throw new IllegalStateException();
        }
    }
}
