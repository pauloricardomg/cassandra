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

package org.apache.cassandra.repair.mutation;

import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDSerializer;

public class MBRCommand
{
    public final UUID cfid;
    public final int nowInSeconds;
    public final MBRRepairPage repairPage;

    public static final IVersionedSerializer<MBRCommand> serializer = new IVersionedSerializer<MBRCommand>()
    {
        public void serialize(MBRCommand mbrCommand, DataOutputPlus out, int version) throws IOException
        {
            MBRRepairPage rp = mbrCommand.repairPage;
            UUIDSerializer.serializer.serialize(mbrCommand.cfid, out, version);
            out.writeInt(mbrCommand.nowInSeconds);
            rp.serialize(out, version);
        }

        public MBRCommand deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID cfid = UUIDSerializer.serializer.deserialize(in, version);
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(cfid);
            assert cfs != null;
            int nowInSeconds = in.readInt();
            MBRRepairPage rp = MBRRepairPage.deserialize(in, cfs.getPartitioner(), version);
            return new MBRCommand(cfid, nowInSeconds, rp);
        }

        public long serializedSize(MBRCommand mbrCommand, int version)
        {
            long size = UUIDSerializer.serializer.serializedSize(mbrCommand.cfid, version);
            size += TypeSizes.sizeof(mbrCommand.nowInSeconds);
            size += mbrCommand.repairPage.serializedSize(version);
            return size;
        }
    };

    public MBRCommand(UUID cfid, int nowInSeconds, MBRRepairPage repairPage)
    {
        this.cfid = cfid;
        this.nowInSeconds = nowInSeconds;
        this.repairPage = repairPage;
    }


    public MessageOut<MBRCommand> createMessage()
    {
        return new MessageOut<>(MessagingService.Verb.MUTATION_REPAIR, this, serializer);
    }

    public String toString()
    {
        return "MutationBasedRepairCommand{" +
               "cfid=" + cfid +
               ", repairPage=" + repairPage +
               '}';
    }
}