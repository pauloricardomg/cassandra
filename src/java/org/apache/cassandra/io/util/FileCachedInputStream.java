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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.cassandra.utils.SyncUtil;

public class FileCachedInputStream extends CachedInputStream
{
    private final File bufferFile;
    private FileOutputStream fileOutputStream;

    public FileCachedInputStream(InputStream in, File bufferFile){
        super(in);
        this.bufferFile = bufferFile;
    }

    public OutputStream createWriteBuffer() throws IOException
    {
        if (bufferFile.exists())
            bufferFile.delete();

        if (!bufferFile.getParentFile().exists())
            bufferFile.getParentFile().mkdirs();

        bufferFile.createNewFile();
        fileOutputStream = new FileOutputStream(bufferFile);
        return new BufferedOutputStream(fileOutputStream);
    }

    public InputStream createReadBuffer() throws IOException
    {
            getWriteBufferIfOpen().flush();
            SyncUtil.sync(fileOutputStream);

            FileInputStream in = new FileInputStream(bufferFile);
            in.skip(bufferFile.length() - getMarkedLength());

            return new BufferedInputStream(in);
    }

    public synchronized void close() throws IOException
    {
        if (!isClosed())
        {
            super.close();
            if (bufferFile.exists())
            {
                bufferFile.delete();
            }
        }
    }
}
