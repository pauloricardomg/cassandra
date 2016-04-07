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

import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class MBRResponse
{
    public static final MBRResponse HUGE = new MBRResponse(null, Type.HUGE);
    public static final MBRResponse MATCH = new MBRResponse(null, Type.MATCH);


    public final ReadResponse response;
    public static final IVersionedSerializer<MBRResponse> serializer = new MBRResponseSerializer();
    public final Type type;


    public MBRResponse(ReadResponse response, Type type)
    {
        this.response = response;
        this.type = type;
    }

    public MessageOut<MBRResponse> createMessage()
    {
        return new MessageOut<>(MessagingService.Verb.INTERNAL_RESPONSE, this, serializer);
    }

    private static class MBRResponseSerializer implements IVersionedSerializer<MBRResponse>
    {
        public void serialize(MBRResponse mutationBasedRepairResponse, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(mutationBasedRepairResponse.type.id);
            mutationBasedRepairResponse.type.serializer.serialize(mutationBasedRepairResponse, out, version);
        }

        public MBRResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            Type type = Type.fromId(in.readInt());
            return type.serializer.deserialize(in, version);
        }

        public long serializedSize(MBRResponse mutationBasedRepairResponse, int version)
        {
            long size = TypeSizes.sizeof(mutationBasedRepairResponse.type.id);
            return size + mutationBasedRepairResponse.type.serializer.serializedSize(mutationBasedRepairResponse, version);
        }
    }

    public enum Type
    {
        MATCH(0, new MatchSerializer()),
        DATA(1, new DataSerializer()),
        HUGE(2, new HugeSerializer());

        private final int id;
        private final IVersionedSerializer<MBRResponse> serializer;

        Type(int i, IVersionedSerializer<MBRResponse> serializer)
        {
            this.id = i;
            this.serializer = serializer;
        }

        static Type fromId(int id)
        {
            for (Type t : values())
                if (t.id == id)
                    return t;
            throw new AssertionError("Type with id="+id+" not found.");
        }

        public static class MatchSerializer implements IVersionedSerializer<MBRResponse>
        {
            public MatchSerializer()
            {
            }
            public void serialize(MBRResponse mutationBasedRepairResponse, DataOutputPlus out, int version) throws IOException {}
            public MBRResponse deserialize(DataInputPlus in, int version) throws IOException
            {
                return MBRResponse.MATCH;
            }

            public long serializedSize(MBRResponse mutationBasedRepairResponse, int version)
            {
                return 0;
            }
        }
        public static class HugeSerializer implements IVersionedSerializer<MBRResponse>
        {
            public HugeSerializer()
            {
            }
            public void serialize(MBRResponse mutationBasedRepairResponse, DataOutputPlus out, int version) throws IOException {}
            public MBRResponse deserialize(DataInputPlus in, int version) throws IOException
            {
                return MBRResponse.HUGE;
            }

            public long serializedSize(MBRResponse mutationBasedRepairResponse, int version)
            {
                return 0;
            }
        }
        public static class DataSerializer implements IVersionedSerializer<MBRResponse>
        {
            public void serialize(MBRResponse mutationBasedRepairResponse, DataOutputPlus out, int version) throws IOException
            {
                ReadResponse.rangeSliceSerializer.serialize(mutationBasedRepairResponse.response, out, version);
            }

            public MBRResponse deserialize(DataInputPlus in, int version) throws IOException
            {
                return new MBRResponse(ReadResponse.rangeSliceSerializer.deserialize(in, version), Type.DATA);
            }

            public long serializedSize(MBRResponse mutationBasedRepairResponse, int version)
            {
                return ReadResponse.rangeSliceSerializer.serializedSize(mutationBasedRepairResponse.response, version);
            }
        }

    }
}