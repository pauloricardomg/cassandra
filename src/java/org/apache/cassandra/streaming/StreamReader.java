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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.UUID;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ning.compress.lzf.LZFInputStream;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.BytesReadTracker;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.utils.Throwables.extractIOExceptionCause;

/**
 * StreamReader reads from stream and writes to SSTable.
 */
public class StreamReader
{
    private static final Logger logger = LoggerFactory.getLogger(StreamReader.class);
    protected final UUID cfId;
    protected final long estimatedKeys;
    protected final Collection<Pair<Long, Long>> sections;
    protected final StreamSession session;
    protected final Descriptor.Version inputVersion;
    protected final long repairedAt;
    protected final int fileSeqNum;

    protected Descriptor desc;

    public StreamReader(FileMessageHeader header, StreamSession session)
    {
        this.session = session;
        this.cfId = header.cfId;
        this.estimatedKeys = header.estimatedKeys;
        this.sections = header.sections;
        this.inputVersion = new Descriptor.Version(header.version);
        this.repairedAt = header.repairedAt;
        this.fileSeqNum = header.sequenceNumber;
    }

    /**
     * @param channel where this reads data from
     * @return SSTable transferred
     * @throws IOException if reading the remote sstable fails. Will throw an RTE if local write fails.
     */
    public SSTableWriter read(ReadableByteChannel channel) throws IOException
    {
        long totalSize = totalSize();

        Pair<String, String> kscf = Schema.instance.getCF(cfId);
        ColumnFamilyStore cfs = null;
        if (kscf != null)
            cfs = Keyspace.open(kscf.left).getColumnFamilyStore(kscf.right);

        if (kscf == null || cfs == null)
        {
            // schema was dropped during streaming
            throw new IOException("CF " + cfId + " was dropped during streaming");
        }

        logger.debug("[Stream #{}] Start receiving file #{} from {}, repairedAt = {}, size = {}, ks = '{}', table = '{}'.",
                     session.planId(), fileSeqNum, session.peer, repairedAt, totalSize, cfs.keyspace.getName(),
                     cfs.getColumnFamilyName());

        DataInputStream dis = new DataInputStream(new LZFInputStream(Channels.newInputStream(channel)));
        BytesReadTracker in = new BytesReadTracker(dis);
        SSTableWriter writer = null;
        DecoratedKey key = null;
        try
        {
            writer = createWriter(cfs, totalSize, repairedAt);
            while (in.getBytesRead() < totalSize)
            {
                key = StorageService.getPartitioner().decorateKey(ByteBufferUtil.readWithShortLength(in));
                writeRow(key, writer, in, cfs);
                // TODO move this to BytesReadTracker
                session.progress(desc, ProgressInfo.Direction.IN, in.getBytesRead(), totalSize);
            }
            logger.debug("[Stream #{}] Finished receiving file #{} from {} readBytes = {}, totalSize = {}",
                         session.planId(), fileSeqNum, session.peer, in.getBytesRead(), totalSize);
            return writer;
        }
        catch (Throwable e)
        {
            if (key != null)
                logger.warn("[Stream {}] Error while reading partition {} from stream on ks='{}' and table='{}'.",
                            session.planId(), key, cfs.keyspace.getName(), cfs.getColumnFamilyName());
            if (writer != null)
            {
                try
                {
                    writer.abort();
                }
                catch (Throwable e2)
                {
                    // add abort error to original and continue so we can drain unread stream
                    e.addSuppressed(e2);
                }
            }
            if (extractIOExceptionCause(e).isPresent())
                throw e;
            //only drain if it's going to retry
            drain(dis, in.getBytesRead());
            throw Throwables.propagate(e);
        }
    }

    protected SSTableWriter createWriter(ColumnFamilyStore cfs, long totalSize, long repairedAt) throws IOException
    {
        Directories.DataDirectory localDir = cfs.directories.getWriteableLocation(totalSize);
        if (localDir == null)
            throw new IOException("Insufficient disk space to store " + totalSize + " bytes");
        desc = Descriptor.fromFilename(cfs.getTempSSTablePath(cfs.directories.getLocationForDisk(localDir)));

        return new SSTableWriter(desc.filenameFor(Component.DATA), estimatedKeys, repairedAt);
    }

    protected void drain(InputStream dis, long bytesRead) throws IOException
    {
        long toSkip = totalSize() - bytesRead;

        // InputStream.skip can return -1 if dis is inaccessible.
        long skipped = dis.skip(toSkip);
        if (skipped == -1)
            return;

        toSkip = toSkip - skipped;
        while (toSkip > 0)
        {
            skipped = dis.skip(toSkip);
            if (skipped == -1)
                break;
            toSkip = toSkip - skipped;
        }
    }

    protected long totalSize()
    {
        long size = 0;
        for (Pair<Long, Long> section : sections)
            size += section.right - section.left;
        return size;
    }

    protected void writeRow(DecoratedKey key, SSTableWriter writer, DataInput in, ColumnFamilyStore cfs) throws IOException
    {
        writer.appendFromStream(key, cfs.metadata, in, inputVersion);
    }
}
