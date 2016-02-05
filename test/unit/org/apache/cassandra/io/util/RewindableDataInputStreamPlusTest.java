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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RewindableDataInputStreamPlusTest
{
    class NonMarkableByteArrayInputStream extends ByteArrayInputStream
    {
        NonMarkableByteArrayInputStream(byte[] buf)
        {
            super(buf);
        }

        public boolean markSupported()
        {
            return false;
        }
    }


    enum SourceStreamType
    {
        BYTEARRAY, BUFFERED, MEMORY, FILE, HYBRID_0, HYBRID_1, HYBRID_2, HYBRID_3, HYBRID_5, HYBRID_8, HYBRID_21, HYBRID_40;

        boolean createsFile() { return this == FILE || this == HYBRID_0; }
        boolean doesNotCreateFile() { return this == BYTEARRAY || this == BUFFERED || this == MEMORY || this == HYBRID_40; }
    }

    private File file;

    @Before
    public void setup() throws Exception
    {
        this.file = new File(System.getProperty("java.io.tmpdir"), "subdir/test.buffer");
    }

    @Test
    public void testMarkAndResetSimple() throws Exception
    {
        internalTestMarkAndResetSimple(SourceStreamType.MEMORY);
        internalTestMarkAndResetSimple(SourceStreamType.FILE);
        internalTestMarkAndResetSimple(SourceStreamType.BYTEARRAY);
        internalTestMarkAndResetSimple(SourceStreamType.HYBRID_0);
        internalTestMarkAndResetSimple(SourceStreamType.HYBRID_1);
        internalTestMarkAndResetSimple(SourceStreamType.HYBRID_2);
        internalTestMarkAndResetSimple(SourceStreamType.HYBRID_3);
        internalTestMarkAndResetSimple(SourceStreamType.HYBRID_5);
        internalTestMarkAndResetSimple(SourceStreamType.HYBRID_8);
        internalTestMarkAndResetSimple(SourceStreamType.HYBRID_21);
        internalTestMarkAndResetSimple(SourceStreamType.HYBRID_40);
        internalTestMarkAndResetSimple(SourceStreamType.BUFFERED);
    }

    @Test
    public void testMarkAndResetBigBuffer() throws Exception
    {
        internalTestMarkAndResetBigBuffer(SourceStreamType.MEMORY);
        internalTestMarkAndResetBigBuffer(SourceStreamType.FILE);
        internalTestMarkAndResetBigBuffer(SourceStreamType.BYTEARRAY);
        internalTestMarkAndResetBigBuffer(SourceStreamType.HYBRID_0);
        internalTestMarkAndResetBigBuffer(SourceStreamType.HYBRID_1);
        internalTestMarkAndResetBigBuffer(SourceStreamType.HYBRID_2);
        internalTestMarkAndResetBigBuffer(SourceStreamType.HYBRID_3);
        internalTestMarkAndResetBigBuffer(SourceStreamType.HYBRID_5);
        internalTestMarkAndResetBigBuffer(SourceStreamType.HYBRID_8);
        internalTestMarkAndResetBigBuffer(SourceStreamType.HYBRID_21);
        internalTestMarkAndResetBigBuffer(SourceStreamType.HYBRID_40);
        internalTestMarkAndResetBigBuffer(SourceStreamType.BUFFERED);
    }

    @Test
    public void testMarkAndResetUnsignedRead() throws Exception
    {
        internalTestMarkAndResetUnsignedRead(SourceStreamType.MEMORY);
        internalTestMarkAndResetUnsignedRead(SourceStreamType.FILE);
        internalTestMarkAndResetUnsignedRead(SourceStreamType.BYTEARRAY);
        internalTestMarkAndResetUnsignedRead(SourceStreamType.HYBRID_0);
        internalTestMarkAndResetUnsignedRead(SourceStreamType.HYBRID_1);
        internalTestMarkAndResetUnsignedRead(SourceStreamType.HYBRID_2);
        internalTestMarkAndResetUnsignedRead(SourceStreamType.HYBRID_3);
        internalTestMarkAndResetUnsignedRead(SourceStreamType.HYBRID_5);
        internalTestMarkAndResetUnsignedRead(SourceStreamType.HYBRID_8);
        internalTestMarkAndResetUnsignedRead(SourceStreamType.HYBRID_21);
        internalTestMarkAndResetUnsignedRead(SourceStreamType.HYBRID_40);
        internalTestMarkAndResetUnsignedRead(SourceStreamType.BUFFERED);
    }

    @Test
    public void testMarkAndResetSkipBytes() throws Exception
    {
        internalTestMarkAndResetSkipBytes(SourceStreamType.MEMORY);
        internalTestMarkAndResetSkipBytes(SourceStreamType.FILE);
        internalTestMarkAndResetSkipBytes(SourceStreamType.BYTEARRAY);
        internalTestMarkAndResetSkipBytes(SourceStreamType.HYBRID_0);
        internalTestMarkAndResetSkipBytes(SourceStreamType.HYBRID_1);
        internalTestMarkAndResetSkipBytes(SourceStreamType.HYBRID_2);
        internalTestMarkAndResetSkipBytes(SourceStreamType.HYBRID_3);
        internalTestMarkAndResetSkipBytes(SourceStreamType.HYBRID_5);
        internalTestMarkAndResetSkipBytes(SourceStreamType.HYBRID_8);
        internalTestMarkAndResetSkipBytes(SourceStreamType.HYBRID_21);
        internalTestMarkAndResetSkipBytes(SourceStreamType.HYBRID_40);
        internalTestMarkAndResetSkipBytes(SourceStreamType.BUFFERED);
    }

    @Test
    public void testMarkAndResetReadFully() throws Exception
    {
        internalTestMarkAndResetReadFully(SourceStreamType.MEMORY);
        internalTestMarkAndResetReadFully(SourceStreamType.FILE);
        internalTestMarkAndResetReadFully(SourceStreamType.BYTEARRAY);
        internalTestMarkAndResetReadFully(SourceStreamType.HYBRID_0);
        internalTestMarkAndResetReadFully(SourceStreamType.HYBRID_1);
        internalTestMarkAndResetReadFully(SourceStreamType.HYBRID_2);
        internalTestMarkAndResetReadFully(SourceStreamType.HYBRID_3);
        internalTestMarkAndResetReadFully(SourceStreamType.HYBRID_5);
        internalTestMarkAndResetReadFully(SourceStreamType.HYBRID_8);
        internalTestMarkAndResetReadFully(SourceStreamType.HYBRID_21);
        internalTestMarkAndResetReadFully(SourceStreamType.HYBRID_40);
        internalTestMarkAndResetReadFully(SourceStreamType.BUFFERED);
    }

    @Test
    public void testConstrainedRewindableDataInputWithoutMarkableParentStream() throws Exception
    {
        internalTestConstrainedCachedInputStream(false);
    }

    @Test
    public void testConstrainedRewindableDataInputWithMarkableParentStream() throws Exception
    {
        internalTestConstrainedCachedInputStream(true);
    }


    public void internalTestConstrainedCachedInputStream(boolean parentMarkable) throws Exception
    {
        byte[] testData;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos))
        {
            // boolean
            out.writeBoolean(true);
            // short
            out.writeShort(1);
            testData = baos.toByteArray();
        }

        for (int capacity = 0; capacity <= 2; capacity++)
        {

            //Test reading without mark/reset
            InputStream sourceStream = parentMarkable? new ByteArrayInputStream(testData) : new NonMarkableByteArrayInputStream(testData);
            CachedInputStream cachedIs = CachedInputStream.newMemoryCachedInputStream(sourceStream, capacity);
            try (RewindableDataInputStreamPlus reader = new RewindableDataInputStreamPlus(cachedIs))
            {
                assertTrue(reader.readBoolean());
                assertEquals(1, reader.readShort());
            }

            //Test caching at most 1 byte (throw IOException otherwise)
            sourceStream = parentMarkable? new ByteArrayInputStream(testData) : new NonMarkableByteArrayInputStream(testData);
            cachedIs = CachedInputStream.newMemoryCachedInputStream(sourceStream, capacity);
            try (RewindableDataInputStreamPlus reader = new RewindableDataInputStreamPlus(cachedIs))
            {
                reader.mark();
                assertTrue(reader.readBoolean());
                try {
                    reader.reset();
                    if (!parentMarkable && capacity == 0)
                        fail("Should have thrown IOException");
                }
                catch (IOException e)
                {
                    if (parentMarkable || capacity > 0)
                        fail("Should have NOT thrown IOException");
                }

                if (parentMarkable || capacity > 0)
                {
                    assertTrue(reader.readBoolean());
                    assertEquals(1, reader.readShort());
                }
            }

            //Test caching at most 2 bytes (throw IOException otherwise)
            sourceStream = parentMarkable? new ByteArrayInputStream(testData) : new NonMarkableByteArrayInputStream(testData);
            cachedIs = CachedInputStream.newMemoryCachedInputStream(sourceStream, capacity);
            try (RewindableDataInputStreamPlus reader = new RewindableDataInputStreamPlus(cachedIs))
            {
                //cache boolean and first byte of short
                reader.mark();
                assertTrue(reader.readBoolean());
                assertEquals(0x0, reader.readByte());

                try {
                    reader.reset();
                    if (!parentMarkable && capacity <= 1)
                        fail("Should have thrown IOException");
                }
                catch (IOException e)
                {
                    if (parentMarkable || capacity > 1)
                        fail("Should have NOT thrown IOException");
                }

                if (parentMarkable || capacity > 1)
                {
                    assertTrue(reader.readBoolean());
                    //cache short
                    reader.mark();
                    assertEquals(0x0, reader.readByte());
                    reader.reset();
                    assertEquals(1, reader.readShort());
                }
            }
        }
    }

    public void internalTestMarkAndResetSimple(SourceStreamType type) throws Exception
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

        InputStream cached = getCachedInputStream(type, testData);

        try (RewindableDataInputStreamPlus reader = new RewindableDataInputStreamPlus(cached))
        {
            try {
                //should mark before resetting
                reader.reset(null);
                if (cached instanceof CachedInputStream)
                    fail("Should have thrown IOException");
            } catch (IOException e) {}

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
                    fail("Should have thrown IOException");
            } catch (IOException e) {}

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
            if (type.createsFile())
                assertEquals(19, file.length()); // 1 (byte) + 2 (char) + 4 (int) + 8 (long) + 4 (float)
            if (type.doesNotCreateFile())
                assertFalse(file.exists());
        }
        assertFalse(file.exists());
    }

    public void internalTestMarkAndResetBigBuffer(SourceStreamType type) throws Exception
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

        InputStream cached = getCachedInputStream(type, testData);

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

            //skip a few
            assertTrue(reader.readBoolean());
            assertEquals(0x1, reader.readByte());
            assertEquals('a', reader.readChar());

            //now let's mark again
            reader.mark();
            assertEquals(1, reader.readShort());
            assertEquals(1, reader.readInt());

            reader.reset(null);
            assertEquals(1, reader.readShort());

            reader.mark();
            assertEquals(1, reader.readInt());
            assertEquals(1L, reader.readLong());
            assertEquals(1.0f, reader.readFloat(), 0);
            assertEquals(1.0d, reader.readDouble(), 0);

            reader.reset();

            reader.mark();
            assertEquals(1, reader.readInt());
            assertEquals(1L, reader.readLong());
            assertEquals(1.0f, reader.readFloat(), 0);
            assertEquals(1.0d, reader.readDouble(), 0);
            assertEquals("abc", reader.readUTF());
            reader.reset();

            assertEquals(1, reader.readInt());
            assertEquals(1L, reader.readLong());
            assertEquals(1.0f, reader.readFloat(), 0);
            assertEquals(1.0d, reader.readDouble(), 0);
            assertEquals("abc", reader.readUTF());

            if (type.createsFile())
            {
                // 1 (boolean) + 1 (byte) + 2 (char) + 2 (short) + 4 (int) + 8 (long)
                // + 4 (float) + 8 (double) + 5 bytes (utf string)
                assertEquals(35, file.length());
            }
            if (type.doesNotCreateFile())
                assertFalse(file.exists());
        }
        assertFalse(file.exists());
    }

    public InputStream getCachedInputStream(SourceStreamType type, byte[] testData)
    {
        InputStream sourceStream = new ByteArrayInputStream(testData);
        switch (type)
        {
            case FILE:
                return CachedInputStream.newFileCachedInputStream(sourceStream, file);
            case MEMORY:
                return CachedInputStream.newMemoryCachedInputStream(sourceStream);
            case HYBRID_0:
                return CachedInputStream.newHybridCachedInputStream(sourceStream, 0, file);
            case HYBRID_1:
                return CachedInputStream.newHybridCachedInputStream(sourceStream, 1, file);
            case HYBRID_2:
                return CachedInputStream.newHybridCachedInputStream(sourceStream, 2, file);
            case HYBRID_3:
                return CachedInputStream.newHybridCachedInputStream(sourceStream, 3, file);
            case HYBRID_5:
                return CachedInputStream.newHybridCachedInputStream(sourceStream, 5, file);
            case HYBRID_8:
                return CachedInputStream.newHybridCachedInputStream(sourceStream, 8, file);
            case HYBRID_21:
                return CachedInputStream.newHybridCachedInputStream(sourceStream, 21, file);
            case HYBRID_40:
                return CachedInputStream.newHybridCachedInputStream(sourceStream, 40, file);
            case BUFFERED:
                return new BufferedInputStream(sourceStream, 1);
            default:
                return new ByteArrayInputStream(testData);
        }
    }

    public void internalTestMarkAndResetUnsignedRead(SourceStreamType type) throws Exception
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

        InputStream cached = getCachedInputStream(type, testData);
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

            //will read first byte from cache, second byte from source stream
            reader.mark();
            assertEquals(2, reader.readUnsignedShort());

            reader.reset();
            assertEquals(2, reader.readUnsignedShort());

            reader.mark();
            reader.reset();
            assertEquals(0, reader.available());

            if (type.createsFile())
                assertEquals(3, file.length()); // 1 (byte) + 2 (short)
            if (type.doesNotCreateFile())
                assertFalse(file.exists());
        }
        assertFalse(file.exists());
    }

    public void internalTestMarkAndResetSkipBytes(SourceStreamType type) throws Exception
    {
        String testStr = "1234567890";
        byte[] testData = testStr.getBytes();

        InputStream cached = getCachedInputStream(type, testData);
        try (RewindableDataInputStreamPlus reader = new RewindableDataInputStreamPlus(cached))
        {
            reader.mark();
            // read first 5 bytes and rewind
            byte[] out = new byte[5];
            reader.readFully(out, 0, 5);
            assertEquals("12345", new String(out));
            reader.reset();

            reader.mark();

            // then skip 7 bytes (12345 (rewinded) + 67 (original stream))
            reader.skipBytes(7);

            reader.reset();

            //now read previously skipped bytes
            out = new byte[7];
            reader.readFully(out);
            assertEquals("1234567", new String(out));

            //skip 1 byte without marking
            reader.skip(1);

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

            if (type.createsFile())
                assertEquals(9, file.length()); // 7 + 2 bytes (1 was skipped)
            if (type.doesNotCreateFile())
                assertFalse(file.exists());
        }
        assertFalse(file.exists());
    }

    public void internalTestMarkAndResetReadFully(SourceStreamType type) throws Exception
    {
        String testStr = "1234567890";
        byte[] testData = testStr.getBytes();

        InputStream cached = getCachedInputStream(type, testData);
        try (RewindableDataInputStreamPlus reader = new RewindableDataInputStreamPlus(cached))
        {
            reader.mark();
            // read first 5 bytes and rewind
            byte[] out = new byte[5];
            reader.readFully(out, 0, 5);
            assertEquals("12345", new String(out));
            reader.reset();

            // read half from cache, half from parent stream
            reader.mark();
            out = new byte[7];
            reader.readFully(out);
            assertEquals("1234567", new String(out));
            reader.reset();

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

            if (type.createsFile())
                assertEquals(10, file.length()); // all bytes were cached
            if (type.doesNotCreateFile())
                assertFalse(file.exists());
        }
        assertFalse(file.exists());
    }
}
