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
package org.apache.cassandra.streaming.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.io.util.DataOutputStreamAndChannel;
import org.apache.cassandra.streaming.StreamReader;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.compress.CompressedStreamReader;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * IncomingFileMessage is used to receive the part(or whole) of a SSTable data file.
 */
public class IncomingFileMessage extends StreamMessage
{
    public static Serializer<IncomingFileMessage> serializer = new Serializer<IncomingFileMessage>()
    {
        public IncomingFileMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException
        {
            DataInputStream input = new DataInputStream(Channels.newInputStream(in));
            FileMessageHeader header = FileMessageHeader.serializer.deserialize(input, version);
            StreamReader reader = header.compressionInfo == null ? new StreamReader(header, session)
                    : new CompressedStreamReader(header, session);

            try
            {
                SSTableWriter sstable = reader.read(in);
                IncomingFileMessage fileMessage = new IncomingFileMessage(sstable, header, reader.getKeysToInvalidate());
                return fileMessage;
            }
            catch (IOException eof)
            {
                // Reading from remote failed(i.e. reached EOF before reading expected length of data).
                // This can be caused by network/node failure thus we are not retrying
                throw eof;
            }
            catch (Throwable t)
            {
                // Throwable can be Runtime error containing IOException.
                // In that case we don't want to retry.
                Throwable cause = t;
                while ((cause = cause.getCause()) != null)
                {
                   if (cause instanceof IOException)
                       throw (IOException) cause;
                }
                JVMStabilityInspector.inspectThrowable(t);
                // Otherwise, we can retry
                session.doRetry(header, t);
                return null;
            }
        }

        public void serialize(IncomingFileMessage message, DataOutputStreamAndChannel out, int version, StreamSession session) throws IOException
        {
            throw new UnsupportedOperationException("Not allowed to call serialize on an incoming file");
        }
    };

    public final FileMessageHeader header;
    public final SSTableWriter sstable;
    public final List<DecoratedKey> keysToInvalidate;

    public IncomingFileMessage(SSTableWriter sstable, FileMessageHeader header, List<DecoratedKey> keysToInvalidate)
    {
        super(Type.FILE);
        this.header = header;
        this.sstable = sstable;
        this.keysToInvalidate = keysToInvalidate;
    }

    @Override
    public String toString()
    {
        return "File (" + header + ", file: " + sstable.getFilename() + ")";
    }
}

