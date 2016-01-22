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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Wrapper input stream that adds rewind functionality to a source input stream via the {@link RewindableDataInput} interface.
 * Furthermore, it implements the {@link DataInputPlus} interface, providing utility methods to read Java primitive types
 * from the stream.
 *
 * The rewind functionality is provided via the following methods:
 * <ul>
 * <li>{@link InputStream#mark(int)} and {@link InputStream#reset()}</li>
 * <li>{@link RewindableDataInput#mark()} and {@link RewindableDataInput#reset(DataPosition)}</li>
 * </ul>
 *
 * The current implementation only allows to mark the current position via {@link this#mark(int)} or
 * {@link this#mark()}, and return later to the last marked position via {@link this#reset()}
 * or {@link this#reset(DataPosition)}. This means that the <code>mark</code> parameter of {@link this#reset(DataPosition)}
 * method is currently ignored.
 *
 * There is no limit to the amount of buffered bytes, so the <code>readlimit</code> parameter of
 * {@link this#mark(int)} is currently ignored.
 */
public class RewindableDataInputStreamPlus extends FilterInputStream implements RewindableDataInput
{
    private final DataInputStreamPlus dataReader;
    ByteArrayOutputStream buffer;
    private int bufferSize;
    private int remaining = 0;
    private boolean marked = false;
    private ByteArrayInputStream cached = null;

    public RewindableDataInputStreamPlus(InputStream in, int bufferSize)
    {
        super(in);
        this.bufferSize = bufferSize;
        dataReader = new DataInputStreamPlus(this);
    }

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

    public int available() throws IOException
    {
        return remaining + super.available();
    }

    /**
     * Marks the current position of a stream to return to this position later via the {@link this#reset()} method.
     * @return An empty @link{DataPosition} object
     */
    public DataPosition mark()
    {
        mark(0);
        return new BufferedDataInputPlusMark();
    }

    /**
     * Marks the current position of a stream to return to this position later via the {@link this#reset(DataPosition)} method.
     * @param readlimit this parameter is currently ignored, so there is no limit to buffered bytes.
     */
    public synchronized void mark(int readlimit)
    {
        if (marked)
            throw new IllegalStateException("Stream is already marked. Reset must be called before marking again.");
        this.marked = true;
        this.buffer = new ByteArrayOutputStream();
    }

    public boolean markSupported()
    {
        return true;
    }

    /**
     * Rewinds to the previously marked position via the {@link this#mark()} method.
     * @throws IOException if an error ocurs while resetting
     */
    public synchronized void reset() throws IOException
    {
        if (!marked)
            throw new IllegalStateException("Stream is not marked. Mark must be called before calling reset.");
        this.marked = false;
        if (buffer.size() > 0) //reset is no-op if buffer is empty
        {
            this.cached = new ByteArrayInputStream(buffer.toByteArray());
            this.remaining = buffer.size();
            this.buffer.reset();
        }
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
        return remaining;
    }

    protected static class BufferedDataInputPlusMark implements DataPosition
    {
    }

    public long skip(long n) throws IOException
    {
        long skippedFrombuffer = remaining;
        if (remaining > 0)
            consumeBuffer(remaining);
        return skippedFrombuffer + super.skip(n - skippedFrombuffer);
    }

    public int read() throws IOException
    {
        int result;
        if (remaining > 0)
        {
            result = cached.read();
            consumeBuffer(1);
        } else
            result = super.read();
        if (marked)
        {
            buffer.write(result);
        }
        return result;
    }

    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    private void consumeBuffer(int readLength)
    {
        assert remaining > 0;
        remaining -= readLength;
        if (remaining == 0)
        {
            cached.reset(); //reset == close - IOException
            cached = null; //let gc do its job
        }
    }

    public int read(byte[] b, int off, int len) throws IOException
    {
        int cacheReadLen = Math.min(remaining, len);
        int result = 0;
        if (cacheReadLen > 0)
        {
            result += cached.read(b, off, cacheReadLen);
            consumeBuffer(len);
            off = off + cacheReadLen;
            len = len - cacheReadLen;
        }

        if (len > 0)
        {
            result += super.read(b, off, len);
            if (marked)
                buffer.write(b, off, len);
        }
        return result;
    }

    public void close() throws IOException
    {
        super.close();
        if (cached != null)
            cached.close();
        if (buffer != null)
            buffer.close();
    }
}
