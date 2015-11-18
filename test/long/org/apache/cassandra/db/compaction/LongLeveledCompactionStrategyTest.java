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
package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertTrue;

public class LongLeveledCompactionStrategyTest
{
    public static final String KEYSPACE1 = "LongLeveledCompactionStrategyTest";
    public static final String CF_STANDARDLVL = "StandardLeveled";

    /**
     * Since we use LongLeveledCompactionStrategyTest CF for every test, we want to clean up after the test.
     */
    @Before
    public void truncateSTandardLeveled()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARDLVL);
        setSkipTopLevelBloomFilterOption(store, false);
        store.truncateBlocking();
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        Map<String, String> leveledOptions = new HashMap<>();
        leveledOptions.put("sstable_size_in_mb", "1");
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARDLVL)
                                                .compaction(CompactionParams.lcs(leveledOptions)));
    }

    @Test
    public void testParallelLeveledCompaction() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARDLVL);
        store.disableAutoCompaction();

        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy)store.getCompactionStrategyManager().getStrategies().get(1);

        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 128;
        int columns = 10;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            UpdateBuilder builder = UpdateBuilder.create(store.metadata, key);
            for (int c = 0; c < columns; c++)
                builder.newRow("column" + c).add("val", value);

            Mutation rm = new Mutation(builder.build());
            rm.apply();
            store.forceBlockingFlush();
        }

        // Execute LCS in parallel
        ExecutorService executor = new ThreadPoolExecutor(4, 4,
                                                          Long.MAX_VALUE, TimeUnit.SECONDS,
                                                          new LinkedBlockingDeque<Runnable>());
        List<Runnable> tasks = new ArrayList<Runnable>();
        while (true)
        {
            while (true)
            {
                final AbstractCompactionTask nextTask = lcs.getNextBackgroundTask(Integer.MIN_VALUE);
                if (nextTask == null)
                    break;
                tasks.add(new Runnable()
                {
                    public void run()
                    {
                        nextTask.execute(null);
                    }
                });
            }
            if (tasks.isEmpty())
                break;

            List<Future<?>> futures = new ArrayList<Future<?>>(tasks.size());
            for (Runnable r : tasks)
                futures.add(executor.submit(r));
            FBUtilities.waitOnFutures(futures);

            tasks.clear();
        }

        // Assert all SSTables are lined up correctly.
        LeveledManifest manifest = lcs.manifest;
        int levels = manifest.getLevelCount();
        for (int level = 0; level <= levels; level++)
        {
            List<SSTableReader> sstables = manifest.getLevel(level);
            // score check
            assert (double) SSTableReader.getTotalBytes(sstables) / LeveledManifest.maxBytesForLevel(level, 1 * 1024 * 1024) < 1.00;
            // overlap check for levels greater than 0
            for (SSTableReader sstable : sstables)
            {
                // level check
                assert level == sstable.getSSTableLevel();

                if (level > 0)
                {// overlap check for levels greater than 0
                    Set<SSTableReader> overlaps = LeveledManifest.overlapping(sstable, sstables);
                    assert overlaps.size() == 1 && overlaps.contains(sstable);
                }
            }
        }
    }

    @Test
    public void testParallelLeveledCompactionWithSkipTopLevelBloomFilterOption() throws Exception
    {
        //enable "skip_top_level_bloom_filter" option
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARDLVL);
        setSkipTopLevelBloomFilterOption(cfs, true);
        cfs.reload();

        //Populate column family and check all assertions from previous test are fine
        testParallelLeveledCompaction();

        //Assert sstables from top level DO NOT HAVE bloom filters (lower levels may or may not have)
        assertTopLevelSStablesDoNotHaveBloomFilter();

        //Reinsert data, to recreate sstables from level L-1 and L-2
        testParallelLeveledCompaction();

        //Now, all the sstables from lower levels must've been re-generated,
        //so they all should have bloom filters, while top level sstables must still
        // not have bloom filters.
        assertTopLevelSStablesDoNotHaveBloomFilterAndLowerLevelSStablesHave();

        //Reload SSTables from disk
        cfs.clearUnsafe();
        cfs.reloadSSTablesUnsafe();

        //Top-level sstables loaded from disk should also not have bloom filters
        assertTopLevelSStablesDoNotHaveBloomFilter();
    }

    private void setSkipTopLevelBloomFilterOption(ColumnFamilyStore cfs, Boolean value)
    {
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class", "LeveledCompactionStrategy");
        localOptions.put("sstable_size_in_mb", "1");
        localOptions.put(LeveledCompactionStrategy.SKIP_TOP_LEVEL_BLOOM_FILTER_OPTION, value.toString());
        cfs.setCompactionParameters(localOptions);
    }

    private void assertTopLevelSStablesDoNotHaveBloomFilter()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARDLVL);
        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy)cfs.getCompactionStrategyManager().getStrategies().get(1);

        assertTrue(Iterables.size(cfs.getSSTables(SSTableSet.CANONICAL)) > 0); //just playing it safe

        int topLevel = lcs.manifest.getLevelCount();
        for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
        {
            long offHeapSize = sstable.getBloomFilter().offHeapSize();
            long serializedSize = sstable.getBloomFilter().serializedSize();
            //System.out.println("* level: " + sstable.getSSTableLevel() + ". offHeapSize: " + offHeapSize);

            //- top level MUST NOT have bloom filter.
            //- lower levels MAY OR MAY NOT have loaded bloom filters,
            //  they will only reload when they participate in the next compaction.
            assertTrue(sstable.getSSTableLevel() == topLevel ? offHeapSize == 0 : offHeapSize >= 0);
            assertTrue(sstable.getSSTableLevel() == topLevel ? serializedSize == 0 : serializedSize >= 0);
        }
    }

    private void assertTopLevelSStablesDoNotHaveBloomFilterAndLowerLevelSStablesHave()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARDLVL);
        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy)cfs.getCompactionStrategyManager().getStrategies().get(1);

        assertTrue(Iterables.size(cfs.getSSTables(SSTableSet.CANONICAL)) > 0); //just playing it safe

        int topLevel = lcs.manifest.getLevelCount();
        assertTrue(Iterables.size(cfs.getSSTables(SSTableSet.CANONICAL)) > 0);
        for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
        {
            long offHeapSize = sstable.getBloomFilter().offHeapSize();
            long serializedSize = sstable.getBloomFilter().serializedSize();
            //System.out.println("* level: " + sstable.getSSTableLevel() + ". offHeapSize: " + offHeapSize);

            //- top level MUST NOT have bloom filter.
            //- lower levels MUST have bloom filter
            assertTrue(sstable.getSSTableLevel() == topLevel ? offHeapSize == 0 : offHeapSize > 0);
            assertTrue(sstable.getSSTableLevel() == topLevel ? serializedSize == 0 : serializedSize > 0);
        }
    }
}