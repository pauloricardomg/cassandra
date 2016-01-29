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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class CachedInputStream extends FilterInputStream
{
    private InputStream readBuffer = null;
    protected OutputStream writeBuffer = null;
    protected long markedPosition = -1;
    protected long position = 0;

    private long remaining = 0;

    public CachedInputStream(InputStream in)
    {
        super(in);
    }

    public abstract OutputStream createWriteBuffer();

    public abstract InputStream internalReset();

    public abstract void resetWriteBuffer();

    /**
     * Marks the current position of a stream to return to this position later via the {@link this#reset()} method.
     *
     * @param readlimit this parameter is currently ignored, so there is no limit to buffered bytes.
     */
    public synchronized void mark(int readlimit)
    {
        if (isMarked())
            throw new IllegalStateException("Stream is already marked. Reset must be called before marking again.");
        this.markedPosition = position;
        if (this.writeBuffer == null)
            writeBuffer = createWriteBuffer();
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
            throw new IllegalStateException("Stream is not marked. Mark must be called before calling reset.");
        if (position > markedPosition) //reset is no-op if posititon <= markedPosition
        {
            closeReadBuffer();
            this.readBuffer = internalReset();
            this.remaining = position - markedPosition;
            position = markedPosition;
            markedPosition = -1;
            resetWriteBuffer();
        }
    }

    public int available() throws IOException
    {
        return (int)remaining + super.available();
    }

    public long skip(long n) throws IOException
    {
        long skippedFrombuffer = remaining;
        incPosition(n);
        if (remaining > 0)
            consumeReadBuffer(remaining);

        return skippedFrombuffer + super.skip(n - skippedFrombuffer);
    }

    public int read() throws IOException
    {
        int result;
        if (remaining > 0)
        {
            result = readBuffer.read();
            consumeReadBuffer(1);
        }
        else
            result = super.read();

        incPosition(1);
        if (shouldWriteToBuffer())
        {
            writeBuffer.write(result);
        }
        return result;
    }

    /**
     * May be overriden by subclasses
     */
    protected boolean shouldWriteToBuffer()
    {
        return this.isMarked();
    }

    public boolean isMarked()
    {
        return markedPosition != -1;
    }

    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
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

    public int read(byte[] b, int off, int len) throws IOException
    {
        int cacheReadLen = (int)Math.min(remaining, (long)len);
        int result = 0;
        if (cacheReadLen > 0)
        {
            result += readBuffer.read(b, off, cacheReadLen);
            consumeReadBuffer(len);
            off = off + cacheReadLen;
            len = len - cacheReadLen;
        }

        incPosition(len);
        if (len > 0)
        {
            result += super.read(b, off, len);
            if (shouldWriteToBuffer())
                writeBuffer.write(b, off, len);
        }
        return result;
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
}