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
import java.io.OutputStream;

import jdk.internal.util.xml.impl.Input;

public class MemoryCachedInputStream extends CachedInputStream
{
    public MemoryCachedInputStream(InputStream in){
        super(in);
    }

    public MemoryCachedInputStream(InputStream in, long capacity){
        super(in, capacity);
    }

    public InputStream createReadBuffer() throws IOException
    {
        return new ByteArrayInputStream(getWriteBuffer().toByteArray());
    }

    public synchronized void reset() throws IOException
    {
        super.reset();
        getWriteBuffer().reset();
    }

    public ByteArrayOutputStream getWriteBuffer() throws IOException
    {
        return (ByteArrayOutputStream) getWriteBufferIfOpen();
    }

    public OutputStream createWriteBuffer()
    {
        return new ByteArrayOutputStream();
    }

    public boolean isEphemeralCache()
    {
        return true;
    }
}
