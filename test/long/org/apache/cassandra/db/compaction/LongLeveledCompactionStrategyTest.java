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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.LogTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LongLeveledCompactionStrategyTest
{
    public static final String KEYSPACE1 = "LongLeveledCompactionStrategyTest";
    public static final String CF_STANDARDLVL = "StandardLeveled";
    public static final Random RANDOM = new Random();

    /**
     * Since we use LongLeveledCompactionStrategyTest CF for every test, we want to clean up after the test.
     */
    @Before
    public void truncateSTandardLeveled()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARDLVL);
        setDisableTopLevelBloomFilterOption(store, false);
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
    @Ignore
    public void testParallelLeveledCompaction() throws Exception
    {
        LeveledCompactionStrategy lcs = insertAndCompact();

        // Assert all SSTables are lined up correctly.
        LeveledManifest manifest = lcs.manifest;
        int levels = manifest.getLevelCount();
        for (int level = 0; level < levels; level++)
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
                    Set<SSTableReader> overlaps = LeveledManifest.overlapping(sstable.first.getToken(), sstable.last.getToken(), sstables);
                    assert overlaps.size() == 1 && overlaps.contains(sstable);
                }
            }
        }
    }

    @Test
    public void testLeveledScanner() throws Exception
    {
        ColumnFamilyStore store = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARDLVL);
        LeveledCompactionStrategy lcs = insertAndCompact();

        ByteBuffer value = ByteBuffer.wrap(new byte[10 * 1024]); // 10 KB value

        // Adds 10 new partitions
        for (int r = 0; r < 10; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            UpdateBuilder builder = UpdateBuilder.create(store.metadata, key);
            for (int c = 0; c < 10; c++)
                builder.newRow("column" + c).add("val", value);

            Mutation rm = new Mutation(builder.build());
            rm.apply();
        }

        //Flush sstable
        store.forceBlockingFlush();

        Iterable<SSTableReader> allSSTables = store.getSSTables(SSTableSet.LIVE);
        for (SSTableReader sstable : allSSTables)
        {
            if (sstable.getSSTableLevel() == 0)
            {
                System.out.println("Mutating L0-SSTABLE level to L1 to simulate a bug: " + sstable.getFilename());
                sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 1);
                sstable.reloadSSTableMetadata();
            }
        }

        try (AbstractCompactionStrategy.ScannerList scannerList = lcs.getScanners(Lists.newArrayList(allSSTables)))
        {
            //Verify that leveled scanners will always iterate in ascending order (CASSANDRA-9935)
            for (ISSTableScanner scanner : scannerList.scanners)
            {
                DecoratedKey lastKey = null;
                while (scanner.hasNext())
                {
                    UnfilteredRowIterator row = scanner.next();
                    if (lastKey != null)
                    {
                        assertTrue("row " + row.partitionKey() + " received out of order wrt " + lastKey, row.partitionKey().compareTo(lastKey) >= 0);
                    }
                    lastKey = row.partitionKey();
                }
            }
        }
    }

    @Test
    public void testDisableTopLevelBloomFilterMinorCompaction() throws Exception
    {
        insertAndCompact();

        assertAllSStablesHaveBloomFilter();

        //here we only enable the option after first leveling
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARDLVL);
        setDisableTopLevelBloomFilterOption(cfs, true);
        cfs.reload();

        assertOnlyTopLevelSStablesDoNotHaveBloomFilter();

        //Reinsert data, to recreate sstables from level L-1 and L-2
        insertAndCompact();

        assertOnlyTopLevelSStablesDoNotHaveBloomFilter();

        checkReloadAndDisable(cfs, true);
    }

    @Test
    public void testDisableTopLevelBloomFilterMajorCompaction() throws Exception
    {
        //set "disable_top_level_bloom_filter" option
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARDLVL);
        setDisableTopLevelBloomFilterOption(cfs, true);
        cfs.reload();

        //Populate table and compact
        insertAndCompact();

        //Perform major compaction, to regenerate all levels
        CompactionManager.instance.performMaximal(cfs, false);

        assertOnlyTopLevelSStablesDoNotHaveBloomFilter();

        checkReloadAndDisable(cfs, false);
    }

    @Test
    public void testDisableTopLevelBloomFilterMajorCompactionOnL0() throws Exception
    {
        //set "disable_top_level_bloom_filter" option
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARDLVL);
        setDisableTopLevelBloomFilterOption(cfs, true);
        cfs.reload();

        cfs.disableAutoCompaction();
        insertData(cfs, 128);

        //Perform major compaction, to regenerate all levels
        CompactionManager.instance.performMaximal(cfs, false);

        assertOnlyTopLevelSStablesDoNotHaveBloomFilter();

        checkReloadAndDisable(cfs, true);
    }

    @Test
    public void testDisableTopLevelBloomFilterAntiCompactionCausingLevelDrop() throws Exception
    {
        //set "disable_top_level_bloom_filter" option
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARDLVL);
        setDisableTopLevelBloomFilterOption(cfs, true);
        cfs.reload();
        LeveledCompactionStrategy repaired = (LeveledCompactionStrategy) cfs.getCompactionStrategyManager().getStrategies().get(0).get(0);
        LeveledCompactionStrategy unrepaired = (LeveledCompactionStrategy) cfs.getCompactionStrategyManager().getStrategies().get(1).get(0);

        //Populate table and compact
        insertAndCompact();

        assertEquals(0, repaired.manifest.getLevelCount());
        assertEquals(3, unrepaired.manifest.getLevelCount());
        assertOnlyTopLevelSStablesDoNotHaveBloomFilter();

        //perform anti-compaction on partial range first
        Collection<SSTableReader> sstables = AntiCompactionTest.getUnrepairedSSTables(cfs);
        Range<Token> range = new Range<Token>(new ByteOrderedPartitioner.BytesToken("0".getBytes()), new ByteOrderedPartitioner.BytesToken("10".getBytes()));
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            CompactionManager.instance.performAnticompaction(cfs, Collections.singleton(range), refs, txn, 1);
        }

        assertOnlyTopLevelSStablesDoNotHaveBloomFilter();

        //perform anti-compaction on another partial range
        sstables = AntiCompactionTest.getUnrepairedSSTables(cfs);
        range = new Range<>(new ByteOrderedPartitioner.BytesToken("40".getBytes()), new ByteOrderedPartitioner.BytesToken("60".getBytes()));
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            CompactionManager.instance.performAnticompaction(cfs, Collections.singleton(range), refs, txn, 1);
        }

        assertOnlyTopLevelSStablesDoNotHaveBloomFilter();

        //perform anti-compaction on full range
        sstables = AntiCompactionTest.getUnrepairedSSTables(cfs);
        range = new Range<Token>(new ByteOrderedPartitioner.BytesToken("0".getBytes()), new ByteOrderedPartitioner.BytesToken("999".getBytes()));
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            CompactionManager.instance.performAnticompaction(cfs, Collections.singleton(range), refs, txn, 1);
        }

        assertEquals(3, repaired.manifest.getLevelCount());
        assertEquals(0, unrepaired.manifest.getLevelCount());
        assertOnlyTopLevelSStablesDoNotHaveBloomFilter();

        //Reinsert data, to recreate sstables from level L-1 and L-2
        insertAndCompact();

        assertEquals(3, repaired.manifest.getLevelCount());
        assertEquals(3, unrepaired.manifest.getLevelCount());
        assertOnlyTopLevelSStablesDoNotHaveBloomFilter();

        //perform anti-compaction, since there will be overlaps when moving sstables from unrepaired to repaired
        //sstables will be dropped to L0, so bloom filter must be regenerated
        sstables = AntiCompactionTest.getUnrepairedSSTables(cfs);
        range = new Range<>(new ByteOrderedPartitioner.BytesToken("0".getBytes()), new ByteOrderedPartitioner.BytesToken("999".getBytes()));
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            CompactionManager.instance.performAnticompaction(cfs, Collections.singleton(range), refs, txn, 1);
        }

        //double check sstables were in fact dropped to L0
        assertTrue(repaired.manifest.generations[0].size() > 0);

        assertEquals(3, repaired.manifest.getLevelCount());
        assertEquals(0, unrepaired.manifest.getLevelCount());
        assertOnlyTopLevelSStablesDoNotHaveBloomFilter();

        checkReloadAndDisable(cfs, false);
    }

    @Test
    public void testDisableTopLevelBloomFilterAntiCompactionAddingNewTopLevel() throws Exception
    {
        //set "disable_top_level_bloom_filter" option
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARDLVL);
        setDisableTopLevelBloomFilterOption(cfs, true);
        cfs.reload();

        LeveledCompactionStrategy repaired = (LeveledCompactionStrategy) cfs.getCompactionStrategyManager().getStrategies().get(0).get(0);
        LeveledCompactionStrategy unrepaired = (LeveledCompactionStrategy) cfs.getCompactionStrategyManager().getStrategies().get(1).get(0);

        //Populate table with smaller rows to create only L0 and L1
        insertAndCompact(10);

        assertEquals(0, repaired.manifest.getLevelCount());
        assertEquals(2, unrepaired.manifest.getLevelCount());
        assertOnlyTopLevelSStablesDoNotHaveBloomFilter();

        //perform anti-compaction on full range
        Collection<SSTableReader> sstables = AntiCompactionTest.getUnrepairedSSTables(cfs);
        Range<Token> range = new Range<Token>(new ByteOrderedPartitioner.BytesToken("0".getBytes()), new ByteOrderedPartitioner.BytesToken("999".getBytes()));
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            CompactionManager.instance.performAnticompaction(cfs, Collections.singleton(range), refs, txn, 1);
        }

        assertEquals(2, repaired.manifest.getLevelCount());
        assertEquals(0, unrepaired.manifest.getLevelCount());
        assertOnlyTopLevelSStablesDoNotHaveBloomFilter();

        //Reinsert data with larger rows, to create unrepaired overlapping levels L1 and L2
        insertAndCompact(100);

        assertEquals(2, repaired.manifest.getLevelCount());
        assertEquals(3, unrepaired.manifest.getLevelCount());
        assertOnlyTopLevelSStablesDoNotHaveBloomFilter();

        // perform anti-compaction, since L2 will be created on repaired set,
        // BFs from L1 (which was previously top-level) must be reloaded
        sstables = AntiCompactionTest.getUnrepairedSSTables(cfs);
        range = new Range<>(new ByteOrderedPartitioner.BytesToken("0".getBytes()), new ByteOrderedPartitioner.BytesToken("999".getBytes()));
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            CompactionManager.instance.performAnticompaction(cfs, Collections.singleton(range), refs, txn, 1);
        }

        assertEquals(3, repaired.manifest.getLevelCount());
        assertEquals(0, unrepaired.manifest.getLevelCount());
        assertOnlyTopLevelSStablesDoNotHaveBloomFilter();

        checkReloadAndDisable(cfs, true);
    }

    private void insertData(ColumnFamilyStore store, int colSizeInKb)
    {
        ByteBuffer value = ByteBuffer.wrap(new byte[colSizeInKb * 1024]); // 100 KB value, make it easy to have multiple files

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
    }

    private void checkReloadAndDisable(ColumnFamilyStore cfs, boolean changeCompactionStrategy)
    {
        LogTransaction.waitForDeletions(); //avoid race when reloading sstables

        //Reload sstables and compaction strategy to simulate a server restart
        cfs.clearUnsafe();
        cfs.reloadSSTablesUnsafe();
        setDisableTopLevelBloomFilterOption(cfs, true); //this will cause compaction to be reloaded

        //Top-level sstables loaded from disk should also not have bloom filters
        assertOnlyTopLevelSStablesDoNotHaveBloomFilter();

        //changing compaction strategy or disabling enable_top_level_bloom_filter
        //has the same effect of reloading bloom filters
        if (changeCompactionStrategy)
            setSizeTieredCompactionStrategy(cfs);
        else
            setDisableTopLevelBloomFilterOption(cfs, false);

        assertAllSStablesHaveBloomFilter();
    }

    private void setDisableTopLevelBloomFilterOption(ColumnFamilyStore cfs, Boolean value)
    {
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class", "LeveledCompactionStrategy");
        localOptions.put("sstable_size_in_mb", "1");
        localOptions.put(LeveledCompactionStrategy.DISABLE_TOP_LEVEL_BLOOM_FILTER_OPTION, value.toString());
        cfs.setCompactionParameters(localOptions);
    }

    private void setSizeTieredCompactionStrategy(ColumnFamilyStore cfs)
    {
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class", "SizeTieredCompactionStrategy");
        cfs.setCompactionParameters(localOptions);
    }

    private void assertOnlyTopLevelSStablesDoNotHaveBloomFilter()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARDLVL);
        assertTrue(Iterables.size(cfs.getSSTables(SSTableSet.LIVE)) > 0); //just playing it safe

        for (int i=0; i<=1; i++) //i=0 repaired, i=1 unrepaired
        {
            for (AbstractCompactionStrategy strat : cfs.getCompactionStrategyManager().getStrategies().get(i))
            {
                LeveledCompactionStrategy lcs = (LeveledCompactionStrategy)strat;
                int levels = lcs.manifest.getLevelCount();
                assertTrue(Iterables.size(cfs.getSSTables(SSTableSet.LIVE)) > 0);
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.LIVE))
                {
                    if (i == 0 && sstable.isRepaired() || i == 1 && !sstable.isRepaired())
                    {
                        long offHeapSize = sstable.getBloomFilter().offHeapSize();

                        if (isTopLevelSStable(lcs, sstable))
                        {
                            assertEquals(0, offHeapSize);
                            assertTrue(new File(sstable.descriptor.filenameFor(Component.FILTER)).exists());
                        }
                        else if (offHeapSize == 0) //lower level may not have BF if it does not overlap with higher level
                        {
                            System.out.println(sstable.getFilename() + " " + sstable.getSSTableLevel() + " " + sstable.isRepaired());
                            assertFalse(lcs.manifest.overlapsWithHigherLevel(sstable, levels));
                        }
                    }
                }
            }
        }
    }

    private void assertAllSStablesHaveBloomFilter()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARDLVL);
        assertTrue(Iterables.size(cfs.getSSTables(SSTableSet.LIVE)) > 0); //just playing it safe

        assertTrue(Iterables.size(cfs.getSSTables(SSTableSet.LIVE)) > 0);
        for (SSTableReader sstable : cfs.getSSTables(SSTableSet.LIVE))
        {
            assertTrue(sstable.getBloomFilter().offHeapSize() > 0);
            assertTrue(new File(sstable.descriptor.filenameFor(Component.FILTER)).exists());
        }
    }

    private boolean isTopLevelSStable(LeveledCompactionStrategy lcs, SSTableReader sstable)
    {
        return sstable.getSSTableLevel() > 0 && sstable.getSSTableLevel() == lcs.manifest.getLevelCount();
    }

    private LeveledCompactionStrategy insertAndCompact()
    {
        return insertAndCompact(100);
    }

    private LeveledCompactionStrategy insertAndCompact(int rowSizeInKb)
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARDLVL);
        store.disableAutoCompaction();
        CompactionStrategyManager mgr = store.getCompactionStrategyManager();
        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy) mgr.getStrategies().get(1).get(0);

        insertData(store, rowSizeInKb);
        parallelLeveledCompaction(mgr);
        return lcs;
    }

    private void parallelLeveledCompaction(CompactionStrategyManager mgr)
    {
        // Execute LCS in parallel
        ExecutorService executor = new ThreadPoolExecutor(4, 4,
                                                          Long.MAX_VALUE, TimeUnit.SECONDS,
                                                          new LinkedBlockingDeque<Runnable>());

        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy) mgr.getStrategies().get(1).get(0);

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
    }
}
