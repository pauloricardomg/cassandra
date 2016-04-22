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

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class RepairInfo
{
    private final UUID parentSessionId;
    private final InetAddress coordinator;

    private final Set<InetAddress> participants;

    public RepairInfo(UUID parentSessionId, InetAddress coordinator, Set<InetAddress> participants)
    {
        this.parentSessionId = parentSessionId;
        this.coordinator = coordinator;
        this.participants = participants;
    }

    public UUID getParentSessionId()
    {
        return parentSessionId;
    }

    public InetAddress getCoordinator()
    {
        return coordinator;
    }

    public Set<InetAddress> getParticipants()
    {
        return participants;
    }
}
