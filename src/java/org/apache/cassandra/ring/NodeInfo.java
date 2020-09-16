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

import java.util.Collection;
import java.util.UUID;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;

public class NodeInfo
{
    protected final UUID id;
    protected final String dc;
    protected final String rack;
    protected final Collection<Token> tokens;
    protected final InetAddressAndPort address;

    public NodeInfo(UUID id, String dc, String rack, Collection<Token> tokens,
                    InetAddressAndPort address)
    {
        this.id = id;
        this.dc = dc;
        this.rack = rack;
        this.tokens = tokens;
        this.address = address;
    }

    public UUID getId()
    {
        return id;
    }

    public String toString()
    {
        return "NodeInfo{" +
               "id=" + id +
               ", dc='" + dc + '\'' +
               ", rack='" + rack + '\'' +
               ", tokens=" + tokens +
               ", address=" + address +
               '}';
    }
}
