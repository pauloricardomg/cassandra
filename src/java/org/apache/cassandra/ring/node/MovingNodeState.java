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

package org.apache.cassandra.ring.node;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.ring.token.VirtualNode;

public class MovingNodeState extends NodeState
{
    private final Token oldToken;
    private final Token newToken;

    public MovingNodeState(Token oldToken, Token newToken)
    {
        super(Status.MOVING);
        this.oldToken = oldToken;
        this.newToken = newToken;
    }

    @Override
    public Collection<VirtualNode> mapToTokenStates(Token currentToken, String dc, String rack, UUID owner)
    {
        assert currentToken.equals(oldToken) : String.format("Node token (%s) is different from old token (%s)", currentToken, oldToken);
        return Arrays.asList(VirtualNode.movingFrom(oldToken, dc, rack, owner), VirtualNode.movingTo(oldToken, newToken, dc, rack, owner));
    }
}
