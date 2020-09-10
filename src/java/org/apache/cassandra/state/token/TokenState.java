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

package org.apache.cassandra.state.token;

import java.util.Objects;
import java.util.UUID;

import org.apache.cassandra.dht.Token;

public class TokenState
{
    protected enum Status
    {
        INITIAL {
            boolean canTransitionFrom(Status status)
            {
                return false;
            }

            boolean canTransitionTo(Status status)
            {
                return status == NORMAL || status == REMOVED || status == MOVING_TO;
            }
        },
        ADDING {
            boolean canTransitionFrom(Status status)
            {
                return status == INITIAL;
            }

            boolean canTransitionTo(Status status)
            {
                return status == NORMAL || status == REMOVED;
            }
        },
        NORMAL {
            boolean canTransitionFrom(Status status)
            {
                return status != REMOVED;
            }

            boolean canTransitionTo(Status status)
            {
                return status != ADDING;
            }
        },
        REPLACING  {
            boolean canTransitionFrom(Status status)
            {
                return status == NORMAL;
            }

            boolean canTransitionTo(Status status)
            {
                return status == NORMAL;
            }
        },
        MOVING_FROM {
            boolean canTransitionFrom(Status status)
            {
                return status == NORMAL;
            }

            boolean canTransitionTo(Status status)
            {
                return status == REMOVED;
            }
        },
        MOVING_TO {
            boolean canTransitionFrom(Status status)
            {
                return status == INITIAL;
            }

            boolean canTransitionTo(Status status)
            {
                return status == NORMAL;
            }
        },
        REMOVING {
            boolean canTransitionFrom(Status status)
            {
                return status == NORMAL;
            }

            boolean canTransitionTo(Status status)
            {
                return status == NORMAL || status == REMOVED;
            }
        },
        REMOVED {
            boolean canTransitionFrom(Status status)
            {
                return true;
            }

            boolean canTransitionTo(Status status)
            {
                return false;
            }
        };

        abstract boolean canTransitionFrom(Status status);

        abstract boolean canTransitionTo(Status status);
    }

    public final Token token;
    public final UUID owner;
    final Status status;

    protected TokenState(Token token, Status status, UUID owner)
    {
        this.token = token;
        this.status = status;
        this.owner = owner;
    }

    public boolean isRemoved()
    {
        return status == TokenState.Status.REMOVED;
    }

    public boolean isMovingTo()
    {
        return status == TokenState.Status.MOVING_TO;
    }

    public boolean canTransitionFrom(TokenState oldState)
    {
        return status.canTransitionFrom(oldState.status) && owner.equals(oldState.owner);
    }

    public boolean canTransitionTo(TokenState newState)
    {
        return owner.equals(newState.owner) && status.canTransitionTo(newState.status);
    }

    public boolean canMoveTo(TokenState newState)
    {
        return status == Status.MOVING_FROM && newState.status == Status.NORMAL && owner.equals(newState.owner);
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

    public String toString()
    {
        return String.format("{\"owner\": %s, \"status\": %s}", token, owner, status);
    }

    public static TokenState initial(Token token, UUID owner)
    {
        return new TokenState(token, Status.INITIAL, owner);
    }

    public static TokenState adding(Token token, UUID owner)
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

    public static TokenState movingFrom(Token oldToken, UUID owner)
    {
        return new TokenState(oldToken, Status.MOVING_FROM, owner);
    }

    public static TokenState movingTo(Token oldToken, Token newToken, UUID owner)
    {
        return new MovingToState(oldToken, newToken, owner);
    }

    public static TokenState removing(Token token, UUID owner)
    {
        return new TokenState(token, Status.MOVING_TO, owner);
    }

    public static TokenState removed(Token token, UUID id)
    {
        return new TokenState(token, Status.REMOVED, id);
    }
}