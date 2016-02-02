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

public abstract class CachedInputStream extends FilterInputStream
{
    public static final long UNLIMITED_CAPACITY = -1;

    private InputStream readBuffer = null;
    protected OutputStream writeBuffer = null;
    protected long markedPosition = -1;
    protected long position = 0;
    private final long capacity;

    private long remaining = 0;
    private long parentMarkedPosition = -1;
    private boolean isMarkInvalidated = false;
    private IOException markException = null;

    public CachedInputStream(InputStream in)
    {
        this(in, UNLIMITED_CAPACITY);
    }

    public CachedInputStream(InputStream in, long capacity)
    {
        super(in);
        this.capacity = capacity;
    }

    public abstract OutputStream createWriteBuffer() throws IOException;

    public abstract InputStream internalReset() throws IOException;

    public abstract void resetWriteBuffer();

    /**
     * Marks the current position of a stream to return to this position later via the {@link this#reset()} method.
     *
     * @param readlimit this parameter is currently ignored, so there is no limit to buffered bytes.
     */
    public synchronized void mark(int readlimit)
    {
        if (isMarked()) //TODO support mark of already marked stream
            throw new IllegalStateException("Stream is already marked. Reset must be called before marking again.");
        this.markedPosition = position;
        if (this.writeBuffer == null)
        {
            try {
                writeBuffer = createWriteBuffer();
            }
            catch (IOException e)
            {
                markException = e;
            }
        }
        internalMark();
    }

    protected void internalMark()
    {
        //no-op, may be overriden my implementations
    }

    public boolean markSupported()
    {
        return true;
    }

    /**
     * Rewinds to the previously marked position via the {@link this#mark(int)} method.
     *
     * @throws IOException if an error ocurs while resetting
     */
    public synchronized void reset() throws IOException
    {
        if (!isMarked())
            throw new IOException("Stream is not marked. Mark must be called before calling reset.");

        if (markException != null)
            throw markException;

        if (isMarkInvalidated)
            throw new IOException(String.format("Mark was invalidated because cache buffer was full. Capacity: %d bytes.",
                                                capacity));

        if (parentMarkedPosition != -1)
            in.reset();

        if (position > markedPosition && parentMarkedPosition != markedPosition) //reset is no-op if posititon <= markedPosition
        {
            closeReadBuffer();
            this.readBuffer = internalReset();
            this.remaining = (isParentMarked() ? parentMarkedPosition : position) - markedPosition;
            resetWriteBuffer();
        }

        position = markedPosition;
        markedPosition = -1;
        this.parentMarkedPosition = -1;
        this.isMarkInvalidated = false;
    }

    public boolean isParentMarked()
    {
        return this.parentMarkedPosition != -1;
    }

    public int available() throws IOException
    {
        return (int)remaining + super.available();
    }

    public long skip(long n) throws IOException
    {
        long skippedFrombuffer = remaining;
        if (skippedFrombuffer > 0)
            consumeReadBuffer(skippedFrombuffer);

        incPosition(n);
        return skippedFrombuffer + super.skip(n - skippedFrombuffer);
    }

    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    public int read(byte[] b, int off, int len) throws IOException
    {
        int result = 0;

        // first try to read from cache
        int cacheReadLen = (int)Math.min(remaining, (long)len);
        if (cacheReadLen > 0)
        {
            result += readBuffer.read(b, off, cacheReadLen);
            consumeReadBuffer(cacheReadLen);
            if (shouldWriteToBuffer(cacheReadLen) && isEphemeralCache())
                writeBuffer.write(b, off, cacheReadLen);
            off = off + cacheReadLen;
            len = len - cacheReadLen;
            incPosition(cacheReadLen);
        }

        // read remaining from parent stream
        if (len > 0)
        {
            maybeMarkParentStream(len);
            result += super.read(b, off, len);
            if (shouldWriteToBuffer(len))
            {
                writeBuffer.write(b, off, len);
            }
            incPosition(len);
        }
        return result;
    }

    public void maybeMarkParentStream(int readLength)
    {
        if (!isMarkInvalidated && !isParentMarked()
                && isMarked() && isWriteBufferFull(readLength))
        {
            if (in.markSupported())
            {
                this.parentMarkedPosition = position;
                in.mark(0);
            }
            else
            {
                this.isMarkInvalidated = true;
            }
        }
    }

    public int read() throws IOException
    {
        int result;


        boolean shouldWriteToBuffer = shouldWriteToBuffer(1);
        if (remaining > 0)
        {
            result = readBuffer.read();
            consumeReadBuffer(1);
            shouldWriteToBuffer &= isEphemeralCache(); //persistent caches do not need to be rewritten
        }
        else
        {
            maybeMarkParentStream(1);
            result = super.read();
        }

        if (shouldWriteToBuffer)
        {
            writeBuffer.write(result);
        }

        incPosition(1);
        return result;
    }

    public boolean shouldWriteToBuffer(int len)
    {
        return isMarked() && !isWriteBufferFull(len) && writeBuffer != null;
    }

    public boolean isEphemeralCache()
    {
        return false; //may be overriden by ephemeral implementations
    }

    public boolean isWriteBufferFull(int len)
    {
        return capacity != UNLIMITED_CAPACITY && getWriteBufferSize() + len > capacity;
    }

    public boolean isMarked()
    {
        return markedPosition != -1;
    }

    private void consumeReadBuffer(long readLength)
    {
        assert remaining > 0;
        remaining -= readLength;
        if (remaining == 0)
        {
            closeReadBuffer();
        }
    }

    private void closeReadBuffer()
    {
        if (readBuffer != null)
        {
            try
            {
                readBuffer.close(); //reset == close - IOException
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            readBuffer = null; //let gc do its job
        }
    }

    public void incPosition(long len)
    {
        position += len;
    }

    public void close() throws IOException
    {
        super.close();
        if (readBuffer != null)
            readBuffer.close();
        if (writeBuffer != null)
            writeBuffer.close();
        cleanup();
    }

    protected void cleanup()
    {
        //may be overriden by implementations
    }

    protected long getWriteBufferSize()
    {
        return position - markedPosition;
    }

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