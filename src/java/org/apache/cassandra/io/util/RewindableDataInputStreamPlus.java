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

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;


/**
 * Adds <code>RewindableDataInput</code> functionality to a source stream with mark/reset support.
 */
public class RewindableDataInputStreamPlus extends DataInputStream implements RewindableDataInput, Closeable, DataInputPlus
{
    private static final int DEFAULT_MAX_READ_AHEAD_BYTES = 1024 * 1024; //1MB
    private final int maxReadAhead;

    public RewindableDataInputStreamPlus(InputStream in, int maxReadAhead)
    {
        super(in);
        assert in.markSupported();
        this.maxReadAhead = maxReadAhead;
    }

    public RewindableDataInputStreamPlus(InputStream in)
    {
        this(in, DEFAULT_MAX_READ_AHEAD_BYTES);
    }

    /**
     * Marks the current position of a stream to return to this position later via the {@link this#reset()} method.
     * @return An empty @link{DataPosition} object
     */
    public DataPosition mark()
    {
        mark(maxReadAhead);
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
        try
        {
            return available();
        }
        catch (IOException e)
        {
            return 0;
        }
    }

    protected static class RewindableDataInputPlusMark implements DataPosition
    {
    }
}
