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

package org.apache.cassandra.ring.token;

import java.util.Objects;
import java.util.UUID;

import org.apache.cassandra.dht.Token;

public class VirtualNode implements Comparable<VirtualNode>
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
                return status == ADDING || status == NORMAL || status == REMOVED || status == MOVING_TO;
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
        REMOVED {
            boolean canTransitionFrom(Status status)
            {
                return true;
            }

            boolean canTransitionTo(Status status)
            {
                return false;
            }
        },
        ADDING(true, REMOVED) {
            boolean canTransitionFrom(Status status)
            {
                return status == INITIAL;
            }

            boolean canTransitionTo(Status status)
            {
                return status == NORMAL || status == REMOVED;
            }
        },
        REPLACING(true, NORMAL)  {
            boolean canTransitionFrom(Status status)
            {
                return status == NORMAL;
            }

            boolean canTransitionTo(Status status)
            {
                return status == NORMAL;
            }
        },
        MOVING_FROM(true, NORMAL) {
            boolean canTransitionFrom(Status status)
            {
                return status == NORMAL;
            }

            boolean canTransitionTo(Status status)
            {
                return status == REMOVED;
            }
        },
        MOVING_TO(true, REMOVED) {
            boolean canTransitionFrom(Status status)
            {
                return status == INITIAL;
            }

            boolean canTransitionTo(Status status)
            {
                return status == NORMAL;
            }
        },
        REMOVING(true, NORMAL) {
            boolean canTransitionFrom(Status status)
            {
                return status == NORMAL;
            }

            boolean canTransitionTo(Status status)
            {
                return status == NORMAL || status == REMOVED;
            }
        };

        final boolean isPending;
        final Status abortedState;

        Status()
        {
            this.isPending = false;
            this.abortedState = null;
        }

        Status(boolean isPending, Status abortedState)
        {
            this.isPending = isPending;
            this.abortedState = abortedState;
        }

        abstract boolean canTransitionFrom(Status status);
        abstract boolean canTransitionTo(Status status);

        public Status abort()
        {
            return abortedState == null ? this : abortedState;
        }
    }

    public final Token token;
    public final String dc;
    public final String rack;
    public final UUID owner;
    final Status status;

    protected VirtualNode(Token token, String dc, String rack, UUID owner, Status status)
    {
        this.token = token;
        this.dc = dc;
        this.rack = rack;
        this.owner = owner;
        this.status = status;
    }
    public boolean isAdding()
    {
        return false;
    }

    public boolean isRemoving()
    {
        return false;
    }

    public boolean isRemoved()
    {
        return status == VirtualNode.Status.REMOVED;
    }

    public boolean isMovingTo()
    {
        return status == VirtualNode.Status.MOVING_TO;
    }

    public boolean canTransitionFrom(VirtualNode oldState)
    {
        return status.canTransitionFrom(oldState.status) && owner.equals(oldState.owner);
    }

    public boolean canTransitionTo(VirtualNode newState)
    {
        return owner.equals(newState.owner) && status.canTransitionTo(newState.status);
    }

    public boolean canMoveTo(VirtualNode newState)
    {
        return status == Status.MOVING_FROM && newState.status == Status.NORMAL && owner.equals(newState.owner);
    }

    public VirtualNode maybeAbort()
    {
        if (!status.isPending)
            return this;

        return withStatus(status.abort());
    }

    private VirtualNode withStatus(Status newStatus)
    {
        return new VirtualNode(token, dc, rack, owner, newStatus);
    }

    public int compareTo(VirtualNode virtualNode)
    {
        return token.compareTo(virtualNode.token);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VirtualNode that = (VirtualNode) o;
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
        return String.format("{\"dc\": %s, \"rack\": %s, \"owner\": %s, \"status\": %s}", dc, rack, owner.toString().substring(0, 8), status);
    }

    public static VirtualNode initial(Token token, String dc, String rack, UUID owner)
    {
        return new VirtualNode(token, dc, rack, owner, Status.INITIAL);
    }

    public static VirtualNode adding(Token token, String dc, String rack, UUID owner)
    {
        return new VirtualNode(token, dc, rack, owner, Status.ADDING);
    }

    public static VirtualNode normal(Token token, String dc, String rack, UUID owner)
    {
        return new VirtualNode(token, dc, rack, owner, Status.NORMAL);
    }

    public static VirtualNode replacing(Token token, String dc, String rack, UUID previousOwner, UUID newOwner)
    {
        return new ReplacingState(token, dc, rack, newOwner, previousOwner);
    }

    public static VirtualNode movingFrom(Token oldToken, String dc, String rack, UUID owner)
    {
        return new VirtualNode(oldToken, dc, rack, owner, Status.MOVING_FROM);
    }

    public static VirtualNode movingTo(Token oldToken, Token newToken, String dc, String rack, UUID owner)
    {
        return new MovingToState(oldToken, newToken, dc, rack, owner);
    }

    public static VirtualNode removing(Token token, String dc, String rack, UUID owner)
    {
        return new VirtualNode(token, dc, rack, owner, Status.REMOVING);
    }

    public static VirtualNode removed(Token token, String dc, String rack, UUID owner)
    {
        return new VirtualNode(token, dc, rack, owner, Status.REMOVED);
    }

    public boolean isPending()
    {
        return status.isPending;
    }
}