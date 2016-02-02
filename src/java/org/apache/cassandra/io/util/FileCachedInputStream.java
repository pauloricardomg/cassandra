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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.cassandra.utils.SyncUtil;

public class FileCachedInputStream extends CachedInputStream
{
    private final File bufferFile;
    private FileOutputStream currentFileOutputStream;
    private long toSkip;
    private long lastReset;
    private long maxReset;

    public FileCachedInputStream(InputStream in, File bufferFile){
        super(in);
        this.bufferFile = bufferFile;
    }

    public InputStream internalReset() throws IOException
    {
            getWriteBuffer().flush();
            SyncUtil.sync(currentFileOutputStream);

            FileInputStream in = new FileInputStream(bufferFile);
            in.skip(toSkip);

            toSkip += position - markedPosition;
            maxReset = Math.max(maxReset, position);
            lastReset = position;

            return new BufferedInputStream(in);
    }

    protected void internalMark()
    {
        if (position < lastReset)
        {
            toSkip -= lastReset - position;
        }
    }

    public void resetWriteBuffer()
    {
    }

    public BufferedOutputStream getWriteBuffer()
    {
        return (BufferedOutputStream)this.writeBuffer;
    }

    public OutputStream createWriteBuffer() throws IOException
    {
        if (bufferFile.exists())
            bufferFile.createNewFile();
        currentFileOutputStream = new FileOutputStream(bufferFile);
        return new BufferedOutputStream(currentFileOutputStream);
    }

    protected void cleanup()
    {
        if (bufferFile.exists())
        {
            bufferFile.delete();
        }
    }
}
