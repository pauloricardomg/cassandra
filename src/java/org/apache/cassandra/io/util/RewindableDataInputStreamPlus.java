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

package org.apache.cassandra.io.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;


/**
 * Adds mark/reset functionality to another input stream by caching read bytes in a memory
 * buffer and spilling to disk if necessary.
 *
 * Up to <code>maxCapacity</code> read bytes will be cached in memory (heap). If more than
 * <code>maxCapacity</code> bytes are read while the stream is marked, the remaining bytes
 * will be cached in the provided <code>spillFile</code> without limit.
 *
 * Please note that spilled bytes are written sequentially to disk and are only cleaned up
 * when the stream is closed, so the disk must have sufficient space to hold up to the
 * amount of bytes of the source input stream.
 */
public class RewindableDataInputStreamPlus extends FilterInputStream implements RewindableDataInput, Closeable
{
    protected boolean marked = false;
    protected int diskMarkPos = -1;

    protected int memAvailable = 0;
    protected int diskAvailable = 0;

    private final int initialBufferSize;
    private final int maxBufferSize;
    protected int pos = 0;
    protected volatile byte memBuffer[];

    private final File spillFile;
    public BufferedOutputStream diskWriteBuffer = null;
    private BufferedInputStream diskReadBuffer = null;

    private AtomicBoolean closed = new AtomicBoolean(false);

    private final DataInputPlus dataReader;

    public RewindableDataInputStreamPlus(InputStream in, int initialBufferSize, int maxBufferSize, File spillFile)
    {
        super(in);
        dataReader = new DataInputStreamPlus(this);
        this.initialBufferSize = initialBufferSize;
        this.maxBufferSize = maxBufferSize;
        this.spillFile = spillFile;
    }

    /**
     * Marks the current position of a stream to return to this position later via the {@link this#reset(DataPosition)} method.
     * @return An empty @link{DataPosition} object
     */
    public DataPosition mark()
    {
        mark(0);
        return new RewindableDataInputPlusMark();
    }

    /**
     * Rewinds to the previously marked position via the {@link this#mark()} method.
     * @param mark it's not possible to return to a custom position, so this parameter is ignored.
     * @throws IOException if an error ocurs while resetting
     */
    public void reset(DataPosition mark) throws IOException
    {
        reset();
    }

    public long bytesPastMark(DataPosition mark)
    {
        return pos;
    }


    protected static class RewindableDataInputPlusMark implements DataPosition
    {
    }

    /* InputStream methods */

    public boolean markSupported()
    {
        return true;
    }

    /**
     * Marks the current position of a stream to return to this position later via the {@link this#reset()} method.
     * @param readlimit this parameter is ignored
     */
    public synchronized void mark(int readlimit)
    {
        if (marked)
            throw new IllegalStateException("Cannot mark already marked stream.");

        if (memAvailable > 0 || diskAvailable > 0)
            throw new IllegalStateException("Can only mark stream after reading previously marked data.");

        pos = 0;
        marked = true;
    }

    public synchronized void reset() throws IOException
    {
        if (!marked)
            throw new IllegalStateException("Must call mark() before calling reset().");

        memAvailable = diskMarkPos != -1? diskMarkPos : pos;

        if (diskMarkPos != -1)
        {
            diskAvailable = pos - diskMarkPos;
            getIfNotClosed(diskWriteBuffer).flush();
            FileInputStream in = new FileInputStream(spillFile);
            getIfNotClosed(in).skip(spillFile.length() - diskAvailable);
            diskReadBuffer = new BufferedInputStream(in);
        }

        pos = 0;
        marked = false;
        diskMarkPos = -1;
    }

    public int available() throws IOException
    {
        return memAvailable + diskAvailable + super.available();
    }

    public int read() throws IOException
    {
        if (memAvailable > 0)
        {
            memAvailable--;
            return getIfNotClosed(memBuffer)[pos++] & 0xff;
        }

        if (diskAvailable > 0)
        {
            diskAvailable--;
            return getIfNotClosed(diskReadBuffer).read();
        }

        int read = getIfNotClosed(in).read();
        if (read == -1)
            return -1;

        if (marked)
        {
            if (pos < maxBufferSize)
            {
                if (memBuffer == null)
                    memBuffer = new byte[initialBufferSize];
                if (pos + 1 >= getIfNotClosed(memBuffer).length)
                    growMemBuffer(1);
                getIfNotClosed(memBuffer)[pos] = (byte)read;
            }
            else
            {
                if (diskMarkPos == -1)
                {
                    diskMarkPos = pos;
                    maybeCreateDiskBuffer();
                }
                getIfNotClosed(diskWriteBuffer).write(read);
            }
            pos++;
        }

        return read;
    }

    private void maybeCreateDiskBuffer() throws IOException
    {
        if (diskWriteBuffer == null)
        {
            if (!spillFile.getParentFile().exists())
                spillFile.getParentFile().mkdirs();
            spillFile.createNewFile();

            this.diskWriteBuffer = new BufferedOutputStream(new FileOutputStream(spillFile));
        }
    }

    public int read(byte[] b, int off, int len) throws IOException
    {
        int totalReadBytes = 0;

        if (memAvailable > 0)
        {
            totalReadBytes += (memAvailable < len) ? memAvailable : len;
            System.arraycopy(memBuffer, pos, b, off, totalReadBytes);
            pos += totalReadBytes;
            memAvailable -= totalReadBytes;
            off += totalReadBytes;
            len -= totalReadBytes;
        }

        if (len > 0 && diskAvailable > 0)
        {
            int readBytes = getIfNotClosed(diskReadBuffer).read(b, off, len);
            totalReadBytes += readBytes;
            diskAvailable -= readBytes;
            off += readBytes;
            len -= readBytes;
        }

        if (len > 0)
        {
            totalReadBytes += getIfNotClosed(in).read(b, off, len);
        }

        if (marked)
        {
            int memWriteBytes = maxBufferSize > pos ? Math.min(totalReadBytes, maxBufferSize - pos) : 0;
            if (memWriteBytes > 0)
            {
                if (memBuffer == null)
                    memBuffer = new byte[initialBufferSize];
                if (pos + memWriteBytes >= getIfNotClosed(memBuffer).length)
                    growMemBuffer(memWriteBytes);
                System.arraycopy(b, off, memBuffer, pos, memWriteBytes);
                off += memWriteBytes;
            }

            if (memWriteBytes < totalReadBytes)
            {
                if (diskMarkPos == -1)
                {
                    diskMarkPos = pos + memWriteBytes;
                    maybeCreateDiskBuffer();
                }
                getIfNotClosed(diskWriteBuffer).write(b, off, totalReadBytes - memWriteBytes);
            }

            pos += totalReadBytes;
        }

        return totalReadBytes;
    }

    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    private void growMemBuffer(int writeSize)
    {
        int newSize = Math.min(2 * (pos + writeSize), maxBufferSize);
        byte newBuffer[] = new byte[newSize];
        System.arraycopy(memBuffer, 0, newBuffer, 0, pos);
        memBuffer = newBuffer;
    }

    public long skip(long n) throws IOException
    {
        long totalSkipped = 0;

        if (memAvailable > 0)
        {
            long skipped = (memAvailable < n) ? memAvailable : n;
            n -= skipped;
            totalSkipped += skipped;
            memAvailable -= skipped;
            pos += skipped;
        }

        if (n > 0 && diskAvailable > 0)
        {
            long skipped = getIfNotClosed(diskReadBuffer).skip(Math.min(n, diskAvailable));
            n -= skipped;
            totalSkipped += skipped;
            diskAvailable -= skipped;
        }

        if (n > 0)
        {
            if (marked)
            {
                //if marked, we need to cache skipped bytes
                while (n-- > 0 && read() != -1)
                {
                    totalSkipped++;
                }
            }
            else
            {
                long skipped = getIfNotClosed(in).skip(n);
                totalSkipped += skipped;
                pos += skipped;
            }
        }

        return totalSkipped;
    }

    private <T> T getIfNotClosed(T in) throws IOException {
        if (closed.get())
            throw new IOException("Stream closed");
        return in;
    }

    public void close() throws IOException
    {
        close(true);
    }

    public void close(boolean closeUnderlying) throws IOException
    {
        if (closed.compareAndSet(false, true))
        {
            Throwable fail = null;
            if (closeUnderlying)
            {
                try
                {
                    super.close();
                }
                catch (IOException e)
                {
                    fail = merge(fail, e);
                }
            }
            try
            {
                if (diskWriteBuffer != null)
                {
                    this.diskWriteBuffer.close();
                    this.diskWriteBuffer = null;
                }
            } catch (IOException e)
            {
                fail = merge(fail, e);
            }
            try {
                if (spillFile.exists())
                {
                    spillFile.delete();
                }
            }
            catch (Throwable e)
            {
                fail = merge(fail, e);
            }
            maybeFail(fail, IOException.class);
        }
    }

    /* DataInputPlus methods */

    public void readFully(byte[] b) throws IOException
    {
        dataReader.readFully(b);
    }

    public void readFully(byte[] b, int off, int len) throws IOException
    {
        dataReader.readFully(b, off, len);
    }

    public int skipBytes(int n) throws IOException
    {
        return dataReader.skipBytes(n);
    }

    public boolean readBoolean() throws IOException
    {
        return dataReader.readBoolean();
    }

    public byte readByte() throws IOException
    {
        return dataReader.readByte();
    }

    public int readUnsignedByte() throws IOException
    {
        return dataReader.readUnsignedByte();
    }

    public short readShort() throws IOException
    {
        return dataReader.readShort();
    }

    public int readUnsignedShort() throws IOException
    {
        return dataReader.readUnsignedShort();
    }

    public char readChar() throws IOException
    {
        return dataReader.readChar();
    }

    public int readInt() throws IOException
    {
        return dataReader.readInt();
    }

    public long readLong() throws IOException
    {
        return dataReader.readLong();
    }

    public float readFloat() throws IOException
    {
        return dataReader.readFloat();
    }

    public double readDouble() throws IOException
    {
        return dataReader.readDouble();
    }

    public String readLine() throws IOException
    {
        return dataReader.readLine();
    }

    public String readUTF() throws IOException
    {
        return dataReader.readUTF();
    }
}
