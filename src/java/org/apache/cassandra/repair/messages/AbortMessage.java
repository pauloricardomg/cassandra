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

package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * Message to cleanup repair resources on replica nodes.
 *
 * @since 2.1.6
 */
public class AbortMessage extends RepairMessage
{
    public static MessageSerializer serializer = new CleanupMessageSerializer();
    public final UUID parentRepairSession;
    public final List<UUID> sessionIds;

    public AbortMessage(UUID parentRepairSession)
    {
        this(parentRepairSession, new LinkedList<>());
    }

    public AbortMessage(UUID parentRepairSession, List<UUID> sessionIds)
    {
        super(Type.ABORT, null);
        this.parentRepairSession = parentRepairSession;
        this.sessionIds = sessionIds;
    }

    public static class CleanupMessageSerializer implements MessageSerializer<AbortMessage>
    {
        public void serialize(AbortMessage message, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(message.parentRepairSession, out, version);
            out.writeInt(message.sessionIds.size());
        }

        public AbortMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID parentRepairSession = UUIDSerializer.serializer.deserialize(in, version);
            int sessionIdCounts = in.readInt();
            List<UUID> sessionIds = new ArrayList<>(sessionIdCounts);
            for (int i=0; i < sessionIdCounts; i++)
                sessionIds.add(UUIDSerializer.serializer.deserialize(in, version));

            return new AbortMessage(parentRepairSession, sessionIds);
        }

        public long serializedSize(AbortMessage message, int version)
        {
            long size = UUIDSerializer.serializer.serializedSize(message.parentRepairSession, version);
            size += TypeSizes.sizeof(message.sessionIds.size());
            for (UUID id : message.sessionIds)
                size += UUIDSerializer.serializer.serializedSize(id, version);
            return size;
        }
    }
}
