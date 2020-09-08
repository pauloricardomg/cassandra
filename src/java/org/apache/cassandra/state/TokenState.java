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

import java.util.Objects;
import java.util.UUID;

import org.apache.cassandra.dht.Token;

public class TokenState
{
    enum Status
    {
        NORMAL,
        ADDING,
        REPLACING,
        MOVING,
        REMOVING,
        REMOVED,
    }

    final Token token;
    final Status status;
    final UUID owner;

    protected TokenState(Token token, Status status, UUID owner)
    {
        this.token = token;
        this.status = status;
        this.owner = owner;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TokenState that = (TokenState) o;
        return Objects.equals(token, that.token) &&
               status == that.status &&
               Objects.equals(owner, that.owner);
    }

    public int hashCode()
    {
        return Objects.hash(token, status, owner);
    }

    public static TokenState bootstrapping(Token token, UUID owner)
    {
        return new TokenState(token, Status.ADDING, owner);
    }

    public static TokenState normal(Token token, UUID owner)
    {
        return new TokenState(token, Status.NORMAL, owner);
    }

    public static TokenState replacing(Token token, UUID previousOwner, UUID newOwner)
    {
        return new ReplacingState(token, newOwner, previousOwner);
    }

    public static TokenState moving(Token oldToken, Token newToken, UUID owner)
    {
        return new MovingState(oldToken, newToken, owner);
    }

    public static TokenState removing(Token token, UUID owner)
    {
        return new TokenState(token, Status.MOVING, owner);
    }

    public static TokenState removed(Token token, UUID id)
    {
        return new TokenState(token, Status.REMOVED, id);
    }
}