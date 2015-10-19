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
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CounterId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class CQLSSTableWriterTest
{
    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.cleanupAndLeaveDirs();
        Keyspace.setInitialized();
        StorageService.instance.initServer();
    }

    @AfterClass
    public static void tearDown()
    {
        Config.setClientMode(false);
    }

    @Test
    public void testUnsortedWriter() throws Exception
    {
        String KS = "cql_keyspace";
        String TABLE = "table1";

        File dataDir = createTempDataDir(KS, TABLE);

        String schema = "CREATE TABLE cql_keyspace.table1 ("
                      + "  k int PRIMARY KEY,"
                      + "  v1 text,"
                      + "  v2 int"
                      + ")";
        String insert = "INSERT INTO cql_keyspace.table1 (k, v1, v2) VALUES (?, ?, ?)";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .withPartitioner(StorageService.instance.getPartitioner())
                                                  .using(insert).build();

        writer.addRow(0, "test1", 24);
        writer.addRow(1, "test2", null);
        writer.addRow(2, "test3", 42);
        writer.addRow(ImmutableMap.<String, Object>of("k", 3, "v2", 12));
        writer.close();

        SSTableLoader loader = new SSTableLoader(dataDir, new SSTableLoader.Client()
        {
            public void init(String keyspace)
            {
                for (Range<Token> range : StorageService.instance.getLocalRanges("cql_keyspace"))
                    addRangeForEndpoint(range, FBUtilities.getBroadcastAddress());
                setPartitioner(StorageService.getPartitioner());
            }

            public CFMetaData getCFMetaData(String keyspace, String cfName)
            {
                return Schema.instance.getCFMetaData(keyspace, cfName);
            }
        }, new OutputHandler.SystemOutput(false, false));

        loader.stream().get();

        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM cql_keyspace.table1;");
        assertEquals(4, rs.size());

        Iterator<UntypedResultSet.Row> iter = rs.iterator();
        UntypedResultSet.Row row;

        row = iter.next();
        assertEquals(0, row.getInt("k"));
        assertEquals("test1", row.getString("v1"));
        assertEquals(24, row.getInt("v2"));

        row = iter.next();
        assertEquals(1, row.getInt("k"));
        assertEquals("test2", row.getString("v1"));
        assertFalse(row.has("v2"));

        row = iter.next();
        assertEquals(2, row.getInt("k"));
        assertEquals("test3", row.getString("v1"));
        assertEquals(42, row.getInt("v2"));

        row = iter.next();
        assertEquals(3, row.getInt("k"));
        assertEquals(null, row.getBytes("v1")); // Using getBytes because we know it won't NPE
        assertEquals(12, row.getInt("v2"));
    }

    @Test
    public void testCounterWriter() throws Exception
    {
        String KS = "cql_keyspace";
        String TABLE = "my_counter";

        File dataDir1 = createTempDataDir(KS, TABLE);

        // Create CQlSStableWriter and increase value of 5 first rows by 5
        CQLSSTableWriter writer = getCounterCqlssTableWriter(dataDir1, true);
        for (int id=0; id < 5; id++)
            writer.addRow(5L, id);
        writer.close();

        // Load created sstable and check all counters were modified correctly
        loadSStables(dataDir1);
        assertAllCountersEqual(5);

        // Increase counters via CQL and check all counters were modified correctly
        // We leave client mode so next counter inserts will use the actual node counter id
        // and not the transient session id (see UpdateParameters.makeCounter)
        Config.setClientMode(false);
        incAllCountersBy(10);
        assertAllCountersEqual(15);

        // Create another CQlSStableWriter and decrease value of 5 first rows by 3
        // We reset the counter session id and set the client mode back to true
        // to simulate another CQLSStableWriter session
        CounterId.resetSessionId();
        Config.setClientMode(true);
        File dataDir2 = createTempDataDir(KS, TABLE);
        writer = getCounterCqlssTableWriter(dataDir2, false);
        for (int id=0; id < 5; id++)
            writer.addRow(3L, id);
        writer.close();

        // Load created sstable and check all counters were modified correctly
        loadSStables(dataDir2);
        assertAllCountersEqual(12);

        // Increase counters via CQL and check all counters were modified correctly
        Config.setClientMode(false);
        incAllCountersBy(10);
        assertAllCountersEqual(22);
        incAllCountersBy(-5);
        assertAllCountersEqual(17);

        // In the next CQLSStableWriter session,
        // we add multiple counter increment statements to the same row (2+3)
        CounterId.resetSessionId();
        Config.setClientMode(true);
        File dataDir3 = createTempDataDir(KS, TABLE);
        writer = getCounterCqlssTableWriter(dataDir3, true);
        for (int id=0; id < 5; id++)
            writer.addRow(2L, id);
        for (int id=0; id < 5; id++)
            writer.addRow(3L, id);
        writer.close();

        // Load created sstable and check all counters were modified correctly
        loadSStables(dataDir3);
        assertAllCountersEqual(22);
    }

    private File createTempDataDir(String KS, String TABLE)
    {
        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();
        return dataDir;
    }

    private void incAllCountersBy(int delta)
    {
        for (int id=0; id < 5; id++)
        {
            QueryProcessor.executeInternal(String.format("UPDATE cql_keyspace.my_counter SET my_counter = my_counter + %d WHERE my_id = %d;", delta, id));
        }
    }

    private void assertAllCountersEqual(long count)
    {
        for (int id=0; id < 5; id++)
        {
            UntypedResultSet rs = QueryProcessor.executeInternal(String.format("select my_counter from cql_keyspace.my_counter where my_id = %d;", id));
            assertEquals(1, rs.size());
            assertEquals(count, rs.one().getLong("my_counter"));
        }
    }

    private CQLSSTableWriter getCounterCqlssTableWriter(File dataDir, boolean add)
    {
        String schema = "CREATE TABLE cql_keyspace.my_counter (" +
                        "  my_id int, " +
                        "  my_counter counter, " +
                        "  PRIMARY KEY (my_id)" +
                        ")";
        String insert = String.format("UPDATE cql_keyspace.my_counter SET my_counter = my_counter %s ? WHERE my_id = ?",
                                      add? "+" : "-");
        return CQLSSTableWriter.builder().inDirectory(dataDir)
                               .forTable(schema)
                               .withPartitioner(StorageService.instance.getPartitioner())
                                          .using(insert).build();
    }

    private void loadSStables(File dataDir) throws InterruptedException, java.util.concurrent.ExecutionException
    {
        SSTableLoader loader = new SSTableLoader(dataDir, new SSTableLoader.Client()
        {
            public void init(String keyspace)
            {
                for (Range<Token> range : StorageService.instance.getLocalRanges("cql_keyspace"))
                    addRangeForEndpoint(range, FBUtilities.getBroadcastAddress());
                setPartitioner(StorageService.getPartitioner());
            }

            public CFMetaData getCFMetaData(String keyspace, String cfName)
            {
                return Schema.instance.getCFMetaData(keyspace, cfName);
            }
        }, new OutputHandler.SystemOutput(false, false));

        loader.stream().get();
    }

    @Test
    public void testSyncWithinPartition() throws Exception
    {
        // Check that the write respect the buffer size even if we only insert rows withing the same partition (#7360)
        // To do that simply, we use a writer with a buffer of 1MB, and write 2 rows in the same partition with a value
        // > 1MB and validate that this created more than 1 sstable.
        File tempdir = Files.createTempDir();
        String schema = "CREATE TABLE ks.test ("
                      + "  k int,"
                      + "  c int,"
                      + "  v blob,"
                      + "  PRIMARY KEY (k,c)"
                      + ")";
        String insert = "INSERT INTO ks.test (k, c, v) VALUES (?, ?, ?)";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(tempdir)
                                                  .forTable(schema)
                                                  .withPartitioner(StorageService.instance.getPartitioner())
                                                  .using(insert)
                                                  .withBufferSizeInMB(1)
                                                  .build();

        ByteBuffer val = ByteBuffer.allocate(1024 * 1050);

        writer.addRow(0, 0, val);
        writer.addRow(0, 1, val);
        writer.close();

        FilenameFilter filterDataFiles = new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return name.endsWith("-Data.db");
            }
        };
        assert tempdir.list(filterDataFiles).length > 1 : Arrays.toString(tempdir.list(filterDataFiles));
    }


    @Test
    public void testSyncNoEmptyRows() throws Exception
    {
        // Check that the write does not throw an empty partition error (#9071)
        File tempdir = Files.createTempDir();
        String schema = "CREATE TABLE ks.test2 ("
                        + "  k UUID,"
                        + "  c int,"
                        + "  PRIMARY KEY (k)"
                        + ")";
        String insert = "INSERT INTO ks.test2 (k, c) VALUES (?, ?)";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(tempdir)
                                                  .forTable(schema)
                                                  .withPartitioner(StorageService.instance.getPartitioner())
                                                  .using(insert)
                                                  .withBufferSizeInMB(1)
                                                  .build();

        for (int i = 0 ; i < 50000 ; i++) {
            writer.addRow(UUID.randomUUID(), 0);
        }
        writer.close();

    }



    private static final int NUMBER_WRITES_IN_RUNNABLE = 10;
    private class WriterThread extends Thread
    {
        private final File dataDir;
        private final int id;
        public volatile Exception exception;

        public WriterThread(File dataDir, int id)
        {
            this.dataDir = dataDir;
            this.id = id;
        }

        @Override
        public void run()
        {
            String schema = "CREATE TABLE cql_keyspace2.table2 ("
                    + "  k int,"
                    + "  v int,"
                    + "  PRIMARY KEY (k, v)"
                    + ")";
            String insert = "INSERT INTO cql_keyspace2.table2 (k, v) VALUES (?, ?)";
            CQLSSTableWriter writer = CQLSSTableWriter.builder()
                    .inDirectory(dataDir)
                    .forTable(schema)
                    .withPartitioner(StorageService.instance.getPartitioner())
                    .using(insert).build();

            try
            {
                for (int i = 0; i < NUMBER_WRITES_IN_RUNNABLE; i++)
                {
                    writer.addRow(id, i);
                }
                writer.close();
            }
            catch (Exception e)
            {
                exception = e;
            }
        }
    }

    @Test
    public void testConcurrentWriters() throws Exception
    {
        String KS = "cql_keyspace2";
        String TABLE = "table2";

        File dataDir = createTempDataDir(KS, TABLE);

        WriterThread[] threads = new WriterThread[5];
        for (int i = 0; i < threads.length; i++)
        {
            WriterThread thread = new WriterThread(dataDir, i);
            threads[i] = thread;
            thread.start();
        }

        for (WriterThread thread : threads)
        {
            thread.join();
            assert !thread.isAlive() : "Thread should be dead by now";
            if (thread.exception != null)
            {
                throw thread.exception;
            }
        }

        SSTableLoader loader = new SSTableLoader(dataDir, new SSTableLoader.Client()
        {
            public void init(String keyspace)
            {
                for (Range<Token> range : StorageService.instance.getLocalRanges("cql_keyspace2"))
                    addRangeForEndpoint(range, FBUtilities.getBroadcastAddress());
                setPartitioner(StorageService.getPartitioner());
            }

            public CFMetaData getCFMetaData(String keyspace, String cfName)
            {
                return Schema.instance.getCFMetaData(keyspace, cfName);
            }
        }, new OutputHandler.SystemOutput(false, false));

        loader.stream().get();

        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM cql_keyspace2.table2;");
        assertEquals(threads.length * NUMBER_WRITES_IN_RUNNABLE, rs.size());
    }
}
