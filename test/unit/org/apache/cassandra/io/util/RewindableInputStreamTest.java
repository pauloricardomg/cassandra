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

public class RewindableInputStreamTest
{
    private File file;

    @Before
    public void setup() throws Exception
    {
        System.setProperty("cassandra.rewindable_is_initial_mem_buffer_size", "1");
        this.file = new File(System.getProperty("java.io.tmpdir"), "subdir/test.buffer");
    }

    @Test
    public void testMarkAndResetSimple() throws Exception
    {
        byte[] testData;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos))
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

        for (int capacity = 0; capacity <= 36; capacity++)
        {
            InputStream cached = new RewindableInputStream(new ByteArrayInputStream(testData), capacity, file);

            try (RewindableDataInputStreamPlus reader = new RewindableDataInputStreamPlus(cached))
            {
                try {
                    //should mark before resetting
                    reader.reset(null);
                    fail("Should have thrown IllegalStateException");
                } catch (IllegalStateException e) {}

                assertTrue(reader.readBoolean());

                reader.mark();

                try {
                    //cannot mark already marked stream
                    reader.mark();
                    fail("Should have thrown IllegalStateException");
                } catch (IllegalStateException e) {}

                assertEquals(0x1, reader.readByte());
                assertEquals('a', reader.readChar());
                reader.reset(null);

                try {
                    //cannot mark when reading from cache
                    reader.mark();
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
            }
            assertFalse(file.exists());
        }
    }

    @Test
    public void testMarkAndResetBigBuffer() throws Exception
    {
        byte[] testData;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos))
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

            // 1 (boolean) + 1 (byte) + 2 (char) + 2 (short) + 4 (int) + 8 (long)
            // + 4 (float) + 8 (double) + 5 bytes (utf string)
        }

        for (int capacity = 0; capacity <= 36; capacity++)
        {
            InputStream cached = new RewindableInputStream(new ByteArrayInputStream(testData), capacity, file);

            try (RewindableDataInputStreamPlus reader = new RewindableDataInputStreamPlus(cached))
            {
                //read a big amount before resetting
                reader.mark();
                assertTrue(reader.readBoolean());
                assertEquals(0x1, reader.readByte());
                assertEquals('a', reader.readChar());
                assertEquals(1, reader.readShort());
                assertEquals(1, reader.readInt());
                assertEquals(1L, reader.readLong());
                reader.reset();

                //read from buffer
                assertTrue(reader.readBoolean());
                assertEquals(0x1, reader.readByte());
                assertEquals('a', reader.readChar());
                assertEquals(1, reader.readShort());
                assertEquals(1, reader.readInt());
                assertEquals(1L, reader.readLong());

                assertEquals(17, reader.available());

                //mark again
                reader.mark();
                assertEquals(1.0f, reader.readFloat(), 0);
                assertEquals(1.0d, reader.readDouble(), 0);
                assertEquals("abc", reader.readUTF());
                reader.reset();

                assertEquals(17, reader.available());

                assertEquals(1.0f, reader.readFloat(), 0);
                assertEquals(1.0d, reader.readDouble(), 0);
                assertEquals("abc", reader.readUTF());
            }
            assertFalse(file.exists());
        }
    }

    @Test
    public void testMarkAndResetUnsignedRead() throws Exception
    {
        byte[] testData;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos))
        {
            // byte
            out.writeByte(0x1);
            // short
            out.writeShort(2);
            testData = baos.toByteArray();
        }

        for (int capacity = 0; capacity <= 4; capacity++)
        {
            InputStream cached = new RewindableInputStream(new ByteArrayInputStream(testData), capacity, file);

            try (RewindableDataInputStreamPlus reader = new RewindableDataInputStreamPlus(cached))
            {
                reader.mark();
                assertEquals(1, reader.readUnsignedByte());
                reader.reset();
                assertEquals(1, reader.readUnsignedByte());

                //will read first byte of short 2
                reader.mark();
                assertEquals(0, reader.readUnsignedByte());
                reader.reset();

                assertEquals(2, reader.readUnsignedShort());

                reader.mark();
                reader.reset();
                assertEquals(0, reader.available());
            }
        }
        assertFalse(file.exists());
    }

    @Test
    public void testMarkAndResetSkipBytes() throws Exception
    {
        String testStr = "1234567890";
        byte[] testData = testStr.getBytes();

        for (int capacity = 0; capacity <= 11; capacity++)
        {
            InputStream cached = new RewindableInputStream(new ByteArrayInputStream(testData), capacity, file);

            try (RewindableDataInputStreamPlus reader = new RewindableDataInputStreamPlus(cached))
            {
                reader.mark();
                // read first 5 bytes and rewind
                byte[] out = new byte[5];
                reader.readFully(out, 0, 5);
                assertEquals("12345", new String(out));

                // then skip 2 bytes (67)
                reader.skipBytes(2);

                reader.reset();

                //now read part of the previously skipped bytes
                out = new byte[5];
                reader.readFully(out);
                assertEquals("12345", new String(out));

                //skip 3 bytes (2 from cache, 1 from stream)
                reader.skip(3);

                // mark and read 2 more bytes
                reader.mark();
                out = new byte[2];
                reader.readFully(out);
                assertEquals("90", new String(out));
                assertEquals(0, reader.available());
                reader.reset();

                //reset and read only the next byte "9" in the third position
                reader.readFully(out, 1, 1);
                assertEquals("99", new String(out));

                //now we read the remainder via readline
                assertEquals(1, reader.available());
                assertEquals("0", reader.readLine());

            }
            assertFalse(file.exists());
        }
    }

    @Test
    public void testMarkAndResetReadFully() throws Exception
    {
        String testStr = "1234567890";
        byte[] testData = testStr.getBytes();

        for (int capacity = 0; capacity <= 11; capacity++)
        {
            InputStream cached = new RewindableInputStream(new ByteArrayInputStream(testData), capacity, file);

            try (RewindableDataInputStreamPlus reader = new RewindableDataInputStreamPlus(cached))
            {
                reader.mark();
                // read first 5 bytes and rewind
                byte[] out = new byte[5];
                reader.readFully(out, 0, 5);
                assertEquals("12345", new String(out));
                reader.reset();

                // read half from cache, half from parent stream
                out = new byte[7];
                reader.readFully(out);
                assertEquals("1234567", new String(out));

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
            }
            assertFalse(file.exists());
        }
    }
}
