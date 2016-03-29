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

import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.cassandra.dht.Token;

public class Ring
{
    enum VirtualNodeStatus
    {
        NONE, JOINING, LEAVING, MOVING_FROM, MOVING_TO, NORMAL
    }

    class VirtualNode implements Comparable<VirtualNode>
    {
        public final Token token;
        public final UUID id;
        public VirtualNodeStatus status;

        VirtualNode(VirtualNode toCopy)
        {
            this(toCopy.token, toCopy.id, toCopy.status);
        }

        VirtualNode(Token token, UUID id)
        {
            this(token, id, VirtualNodeStatus.NONE);
        }

        VirtualNode(Token token, UUID id, VirtualNodeStatus status)
        {
            this.token = token;
            this.id = id;
            this.status = status;
        }

        public VirtualNodeStatus getStatus()
        {
            return status;
        }

        public void setStatus(VirtualNodeStatus status)
        {
            this.status = status;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            VirtualNode node = (VirtualNode) o;

            if (!token.equals(node.token)) return false;
            return id.equals(node.id);
        }

        public int hashCode()
        {
            int result = token.hashCode();
            result = 31 * result + id.hashCode();
            return result;
        }

        public int compareTo(VirtualNode that)
        {
            if (this == that) return 0;

            int cmp = this.token.compareTo(that.token);
            if (cmp != 0) return cmp;

            //pending statuses come before NORMAL
            return this.status.compareTo(that.status);
        }
    }

    class VirtualNodeIterator implements Iterator<VirtualNode>
    {

        public boolean hasNext()
        {
            return false;
        }

        public VirtualNode next()
        {
            return null;
        }
    }

    private final NavigableSet<VirtualNode> ring;

    public Ring()
    {
        ring = new TreeSet<>();
    }

    public boolean add(VirtualNode node)
    {
        return ring.add(node);
    }

    private Ring(Ring other)
    {
        this();
        for (VirtualNode vn : other.ring)
            ring.add(new VirtualNode(vn));
    }

    public Ring copy()
    {
        return new Ring(this);
    }
}
