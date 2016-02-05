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

import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper input stream that adds mark/reset functionality to a source input stream.
 *
 * If the cached input stream is capacity-constrained, it will mark the parent stream when it runs
 * out of capacity, allowing multiple <code>CachedInputStream</code> implementations to be cascaded
 * to provide a hierarchy of cached input streams.
 *
 */
public abstract class CachedInputStream extends FilterInputStream
{
    protected static final long UNLIMITED_CAPACITY = -1;

    protected final long capacity;

    private OutputStream writeBuffer = null;
    private InputStream readBuffer = null;

    protected long parentMarkPos = -1;
    protected long markPos = -1;
    protected long pos = 0;

    private long available = 0;
    private IOException markException = null;

    private volatile boolean closed = false;

    public CachedInputStream(InputStream in)
    {
        this(in, UNLIMITED_CAPACITY);
    }

    public CachedInputStream(InputStream in, long capacity)
    {
        super(in);
        this.capacity = capacity;
    }

    /* InputStream methods */

    public boolean markSupported()
    {
        return true;
    }

    /**
     * Marks the current position of a stream to return to this position later via the {@link this#reset()} method.
     *
     * @param readlimit this parameter is ignored, so there is no limit to buffered bytes.
     */
    public synchronized void mark(int readlimit)
    {
        if (isMarked())
            throw new IllegalStateException("Stream is already marked. Reset must be called before marking again.");

        if (!isClosed())
        {
            this.markPos = pos;
            if (available > 0)
            {
                readBuffer.mark(readlimit);
            }
            if (this.writeBuffer == null)
            {
                try
                {
                    this.writeBuffer = createWriteBuffer();
                }
                catch (IOException e)
                {
                    this.markPos = -1; //invalidate mark
                    this.markException = e; //exception will be thrown on reset() call
                }
            }
        }
    }

    /**
     * Rewinds to the previously marked position via the {@link this#mark(int)} method.
     *
     * @throws IOException if an error ocurs while resetting
     */
    public synchronized void reset() throws IOException
    {
        checkClosed();

        if (markException != null)
        {
            IOException exception = markException;
            markException = null;
            throw exception;
        }

        if (!isMarked())
            throw new IOException("Stream is not marked or was invalidated. Mark must be called before calling reset.");

        if (parentMarkPos != -1)
            getInIfOpen().reset();

        if (pos > markPos && parentMarkPos != markPos)
        {
            if (available > 0)
            {
                this.readBuffer.reset();
                this.available += pos - markPos;
            }
            if (available == 0)
            {
                closeReadBuffer();
                this.readBuffer = createReadBuffer();
                this.available = (isParentMarked() ? parentMarkPos : pos) - markPos;
            }
        }

        this.pos = markPos;
        this.markPos = -1;
        this.parentMarkPos = -1;
    }



    public int available() throws IOException
    {
        return (int) available + super.available();
    }

    public long skip(long n) throws IOException
    {
        long skipped = 0;

        if (isMarked())
            while (read() != -1 && ++skipped < n);
        else
        {
            if (available > 0)
            {
                skipped += getReadBufferIfOpen().skip(Math.min(available, n));
                available -= skipped;
            }
            skipped += super.skip(Math.max(n - skipped, 0));
            pos += skipped;
        }
        return skipped;
    }

    public int read() throws IOException
    {
        int result;

        boolean shouldWriteToBuffer = shouldWriteToBuffer(1);
        if (available > 0)
        {
            result = getReadBufferIfOpen().read();
            available--;
            shouldWriteToBuffer &= isEphemeralCache(); //persistent caches do not need to be rewritten
        }
        else
        {
            maybeMarkParentStream(1);
            result = super.read();
        }

        if (shouldWriteToBuffer)
        {
            getWriteBufferIfOpen().write(result);
        }

        pos += 1;
        return result;
    }

    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    public int read(byte[] b, int off, int len) throws IOException
    {
        int result = 0;

        // first try to read from cache
        int cacheReadLen = (int)Math.min(available, (long)len);
        if (cacheReadLen > 0)
        {
            result += getReadBufferIfOpen().read(b, off, cacheReadLen);
            available -= cacheReadLen;
            if (shouldWriteToBuffer(cacheReadLen) && isEphemeralCache())
                getWriteBufferIfOpen().write(b, off, cacheReadLen);
            off = off + cacheReadLen;
            len = len - cacheReadLen;
            pos += cacheReadLen;
        }

        // read available from parent stream
        if (len > 0)
        {
            maybeMarkParentStream(len);
            int readBytes = super.read(b, off, len);
            if (shouldWriteToBuffer(readBytes))
            {
                getWriteBufferIfOpen().write(b, off, readBytes);
            }
            pos += readBytes;
            result += readBytes;
        }
        return result;
    }

    public synchronized void close() throws IOException
    {
        if (!isClosed())
        {
            closed = true;
            super.close();
            closeReadBuffer();
            closeWriteBuffer();
        }
    }

    /* Abstract methods */

    public abstract OutputStream createWriteBuffer() throws IOException;

    public abstract InputStream createReadBuffer() throws IOException;

    /* Protected methods */

    protected boolean isClosed()
    {
        return this.closed;
    }

    protected InputStream getInIfOpen() throws IOException {
        checkClosed();
        return in;
    }

    protected OutputStream getWriteBufferIfOpen() throws IOException {
        checkClosed();
        return writeBuffer;
    }

    protected InputStream getReadBufferIfOpen() throws IOException {
        checkClosed();
        return readBuffer;
    }

    protected boolean isEphemeralCache()
    {
        return false; //may be overriden by ephemeral implementations
    }


    protected long getMarkedLength()
    {
        return pos - markPos;
    }

    /* Private methods */

    private boolean isParentMarked()
    {
        return this.parentMarkPos != -1;
    }

    private void maybeMarkParentStream(int readLength)
    {
        if (!isParentMarked()
                && isMarked() && isWriteBufferFull(readLength))
        {
            if (in.markSupported())
            {
                this.parentMarkPos = pos;
                in.mark(0);
            }
            else
            {
                this.markPos = -1; //invalidate mark
            }
        }
    }

    private boolean shouldWriteToBuffer(int len) throws IOException
    {
        return isMarked() && !isWriteBufferFull(len) && getWriteBufferIfOpen() != null;
    }

    private boolean isWriteBufferFull(int len)
    {
        return capacity != UNLIMITED_CAPACITY && getMarkedLength() + len > capacity;
    }

    private boolean isMarked()
    {
        return markPos != -1;
    }

    private void closeReadBuffer() throws IOException
    {
        if (readBuffer != null)
        {
            readBuffer.close();
            readBuffer = null;
        }
    }

    private void closeWriteBuffer() throws IOException
    {
        if (writeBuffer != null)
        {
            writeBuffer.close();
            writeBuffer = null;
        }
    }

    private void checkClosed() throws IOException
    {
        if (isClosed())
            throw new IOException("Stream closed");
    }

    /* Static builders */

    public static CachedInputStream newHybridCachedInputStream(InputStream sourceStream, long memoryCapacityInBytes,
                                                               File bufferFile)
    {
        return new MemoryCachedInputStream(new FileCachedInputStream(sourceStream, bufferFile), memoryCapacityInBytes);
    }

    public static CachedInputStream newMemoryCachedInputStream(InputStream sourceStream, long capacity)
    {
        return new MemoryCachedInputStream(sourceStream, capacity);
    }

    public static CachedInputStream newMemoryCachedInputStream(InputStream sourceStream)
    {
        return new MemoryCachedInputStream(sourceStream);
    }

    public static CachedInputStream newFileCachedInputStream(InputStream sourceStream, File bufferFile)
    {
        return new FileCachedInputStream(sourceStream, bufferFile);
    }
}