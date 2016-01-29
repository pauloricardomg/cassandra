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
import java.io.DataOutputStream;
import java.io.File;
import java.io.InputStream;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RewindableDataInputStreamPlusTest
{

    enum SourceStreamType
    {
        BYTEARRAY, MEMORY, FILE;

        boolean isFile() { return this == FILE; }
    }

    private File file;

    @Before
    public void setup() throws Exception
    {
        this.file = new File("/tmp/bla");
        this.file.createNewFile();
    }

    @Test
    public void testMarkAndResetSimple() throws Exception
    {
        internalTestMarkAndResetSimple(SourceStreamType.FILE);
        internalTestMarkAndResetSimple(SourceStreamType.MEMORY);
        internalTestMarkAndResetSimple(SourceStreamType.BYTEARRAY);
    }

    @Test
    public void testMarkAndResetUnsignedRead() throws Exception
    {
        internalTestMarkAndResetUnsignedRead(SourceStreamType.FILE);
        internalTestMarkAndResetUnsignedRead(SourceStreamType.MEMORY);
        internalTestMarkAndResetUnsignedRead(SourceStreamType.BYTEARRAY);
    }

    @Test
    public void testMarkAndResetSkipBytesAndReadFully() throws Exception
    {
        internalTestMarkAndResetSkipBytesAndReadFully(SourceStreamType.FILE);
        internalTestMarkAndResetSkipBytesAndReadFully(SourceStreamType.MEMORY);
        internalTestMarkAndResetSkipBytesAndReadFully(SourceStreamType.BYTEARRAY);
    }

    public void internalTestMarkAndResetSimple(SourceStreamType type) throws Exception
    {
        byte[] testData;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        try
        {
            // boolean
            out.writeBoolean(true);
            // byte
            out.writeByte(0x1);
            // char
            out.writeChar('a');
            // short
            out.writeShort(1);
            // int
            out.writeInt(1);
            // long
            out.writeLong(1L);
            // float
            out.writeFloat(1.0f);
            // double
            out.writeDouble(1.0d);

            // String
            out.writeUTF("abc");
            testData = baos.toByteArray();
        }
        finally
        {
            out.close();
        }

        InputStream cached = getInputStream(type, testData);
        RewindableDataInputStreamPlus reader = new RewindableDataInputStreamPlus(cached);

        try
        {
            try {
                //should mark before resetting
                reader.reset(null);
                if (cached instanceof CachedInputStream)
                    fail("Should have thrown IllegalStateException");
            } catch (IllegalStateException e) {}

            assertTrue(reader.readBoolean());

            reader.mark();

            try {
                //cannot mark already marked stream
                reader.mark();
                if (cached instanceof CachedInputStream)
                    fail("Should have thrown IllegalStateException");
            } catch (IllegalStateException e) {}

            assertEquals(0x1, reader.readByte());
            assertEquals('a', reader.readChar());
            reader.reset(null);

            try {
                //should mark before resetting
                reader.reset(null);
                if (cached instanceof CachedInputStream)
                    fail("Should have thrown IllegalStateException");
            } catch (IllegalStateException e) {}

            //read again previous sequence
            assertEquals(0x1, reader.readByte());
            assertEquals('a', reader.readChar());
            //finish reading again previous sequence
            assertEquals(1, reader.readShort());

            reader.mark();
            assertEquals(1, reader.readInt());
            assertEquals(1L, reader.readLong());
            assertEquals(1.0f, reader.readFloat(), 0);
            reader.reset(null);

            //read again previous sequence
            assertEquals(1, reader.readInt());
            assertEquals(1L, reader.readLong());
            assertEquals(1.0f, reader.readFloat(), 0);
            //finish reading again previous sequence

            //mark and reset
            reader.mark();
            reader.reset(null);

            assertEquals(1.0d, reader.readDouble(), 0);
            assertEquals("abc", reader.readUTF());
            if (type.isFile())
                assertEquals(19, file.length()); // 1 (byte) + 2 (char) + 4 (int) + 8 (long) + 4 (float)
        }
        finally
        {
            reader.close();
            if (type.isFile())
                assertFalse(file.exists());
        }
    }

    public InputStream getInputStream(SourceStreamType type, byte[] testData)
    {
        switch (type)
        {
            case FILE:
                return new FileCachedInputStream(new ByteArrayInputStream(testData), file);
            case MEMORY:
                return new MemoryCachedInputStream(new ByteArrayInputStream(testData));
            default:
                return new ByteArrayInputStream(testData);
        }
    }

    public void internalTestMarkAndResetUnsignedRead(SourceStreamType type) throws Exception
    {
        byte[] testData;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        try
        {
            // byte
            out.writeByte(0x1);
            // short
            out.writeShort(2);
            testData = baos.toByteArray();
        }
        finally
        {
            out.close();
        }

        InputStream cached = getInputStream(type, testData);
        RewindableDataInputStreamPlus reader = new RewindableDataInputStreamPlus(cached);

        try
        {
            reader.mark();
            assertEquals(1, reader.readUnsignedByte());
            reader.reset();
            assertEquals(1, reader.readUnsignedByte());

            //will read first byte of short 2
            reader.mark();
            assertEquals(0, reader.readUnsignedByte());
            reader.reset();

            //will read first byte from cache, second byte from source stream
            reader.mark();
            assertEquals(2, reader.readUnsignedShort());

            reader.reset();
            assertEquals(2, reader.readUnsignedShort());

            reader.mark();
            reader.reset();
            assertEquals(0, reader.available());

            if (type.isFile())
                assertEquals(3, file.length()); // 1 (byte) + 2 (short)
        }
        finally
        {
            reader.close();
            if (type.isFile())
                assertFalse(file.exists());
        }
    }

    public void internalTestMarkAndResetSkipBytesAndReadFully(SourceStreamType type) throws Exception
    {
        String testStr = "1234567890";
        byte[] testData = testStr.getBytes();

        InputStream cached = getInputStream(type, testData);
        RewindableDataInputStreamPlus reader = new RewindableDataInputStreamPlus(cached);

        try
        {
            reader.mark();
            // read first 5 bytes and rewind
            byte[] out = new byte[5];
            reader.readFully(out, 0, 5);
            assertEquals("12345", new String(out));
            reader.reset();

            // then skip 7 bytes (12345 (rewinded) + 67)
            reader.skipBytes(7);

            // mark and read 3 more bytes
            reader.mark();
            out = new byte[3];
            reader.readFully(out);
            assertEquals("890", new String(out));
            assertEquals(0, reader.available());
            reader.reset();

            //reset and read only the next byte "8" in the third position
            reader.readFully(out, 2, 1);
            assertEquals("898", new String(out));

            //now we read the remainder via readline
            assertEquals(2, reader.available());
            assertEquals("90", reader.readLine());

            if (type.isFile())
                assertEquals(8, file.length()); // 5 + 3 bytes
        }
        finally
        {
            reader.close();
            if (type.isFile())
                assertFalse(file.exists());
        }
    }
}
