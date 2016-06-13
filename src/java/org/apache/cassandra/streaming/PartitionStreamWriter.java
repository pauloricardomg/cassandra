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

package org.apache.cassandra.streaming;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ning.compress.lzf.LZFOutputStream;
import org.apache.cassandra.db.ColumnIndex;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.utils.Pair;

public class PartitionStreamWriter extends StreamWriter
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionStreamWriter.class);
    private final Collection<Range<Token>> ranges;
    private final UnfilteredRowIteratorStreamSerializer serializer;

    public PartitionStreamWriter(SSTableReader sstable, Collection<Pair<Long, Long>> sections,
                                 StreamSession session, Collection<Range<Token>> ranges,
                                 Version version)
    {
        super(sstable, sections, session);
        this.ranges = ranges;
        this.serializer = new NewFormatSerializer(SerializationHeader.make(sstable.metadata, Collections.singleton(sstable)),
                                                  version.correspondingMessagingVersion());
    }

    @Override
    public void write(DataOutputStreamPlus output) throws IOException
    {
        WrappedDataOutputStreamPlus compressed = new WrappedDataOutputStreamPlus(new LZFOutputStream(output));
        try (ISSTableScanner scanner = sstable.getScanner(ranges, null))
        {
            while (scanner.hasNext())
            {
                serializer.serialize(scanner.next(), compressed);
            }
        }
    }

    public static class NewFormatSerializer implements UnfilteredRowIteratorStreamSerializer
    {
        private final SerializationHeader header;
        private final int version;
        private final UnfilteredSerializer serializer = UnfilteredSerializer.serializer;

        private NewFormatSerializer(SerializationHeader header, int version)
        {
            this.header = header;
            this.version = version;
        }

        public void serialize(UnfilteredRowIterator iterator, DataOutputStreamPlus writer) throws IOException
        {
            ColumnIndex.writePartitionHeader(iterator, writer, header, version);
            long previousSize = 0;
            while (iterator.hasNext())
            {
                Unfiltered next = iterator.next();
                serializer.serialize(next, header, writer, previousSize, version);
                previousSize = serializer.serializedSize(next, header, previousSize, version);
            }
            UnfilteredSerializer.serializer.writeEndOfPartition(writer);
            writer.flush();
        }
    }

    public static interface UnfilteredRowIteratorStreamSerializer
    {
        void serialize(UnfilteredRowIterator iterator, DataOutputStreamPlus output) throws IOException;
    }
}
