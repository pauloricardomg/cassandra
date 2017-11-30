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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.io.Files;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class CompactionStrategyManagerTest
{
    private static final String KS_PREFIX = "Keyspace1";
    private static final String TABLE_PREFIX = "CF_STANDARD";

    private static IPartitioner originalPartitioner;
    private static boolean backups;

    @BeforeClass
    public static void beforeClass()
    {
        SchemaLoader.prepareServer();
        backups = DatabaseDescriptor.isIncrementalBackupsEnabled();
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);
        /**
         * We use byte ordered partitioner in this test to be able to easily infer an SSTable
         * disk assignment based on its generation - See {@link this#getSSTableIndex(Integer[], SSTableReader)}
         */
        originalPartitioner = StorageService.instance.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
    }

    @AfterClass
    public static void afterClass()
    {
        DatabaseDescriptor.setPartitionerUnsafe(originalPartitioner);
        DatabaseDescriptor.setIncrementalBackupsEnabled(backups);
    }

    @Test
    public void testSSTablesAssignedToCorrectCompactionStrategy()
    {
        // Creates 100 SSTables with keys 0-99
        int numSSTables = 100;
        SchemaLoader.createKeyspace(KS_PREFIX,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KS_PREFIX, TABLE_PREFIX)
                                                .compaction(CompactionParams.scts(Collections.emptyMap())));
        ColumnFamilyStore cfs = Keyspace.open(KS_PREFIX).getColumnFamilyStore(TABLE_PREFIX);
        cfs.disableAutoCompaction();
        for (int i = 0; i < numSSTables; i++)
        {
            createSSTableWithKey(KS_PREFIX, TABLE_PREFIX, i);
        }

        // Creates a CompactionStrategymanager with different numbers of disks and check
        // if the SSTables are assigned to the correct compaction strategies
        for (int numDisks = 2; numDisks < 10; numDisks++)
        {
            testSSTablesAssignedToCorrectCompactionStrategy(numSSTables, numDisks);
        }
    }

    public void testSSTablesAssignedToCorrectCompactionStrategy(int numSSTables, int numDisks)
    {
        // Create a mock CFS with the given number of disks
        MockCFS cfs = createJBODMockCFS(numDisks);
        //Check that CFS will contain numSSTables
        assertEquals(numSSTables, cfs.getLiveSSTables().size());

        // Creates a compaction strategy manager with a mock boundary manager
        MockBoundaryManager mockBoundaryManager = new MockBoundaryManager(cfs, numSSTables, numDisks);
        CompactionStrategyManager csm = new CompactionStrategyManager(cfs, mockBoundaryManager::getBoundaries,
                                                                      true);

        // Check that SSTables are assigned to the correct Compaction Strategy
        for (SSTableReader reader : cfs.getLiveSSTables())
        {
            verifySSTableIsAssignedOnlyToCorrectStrategy(csm, reader);
        }

        for (int delta = 1; delta <= 3; delta++)
        {
            // Update disk boundaries
            updateBoundaries(mockBoundaryManager, delta);

            for (SSTableReader reader : cfs.getLiveSSTables())
            {
                // Check that SSTables are assigned to the new boundary layout
                verifySSTableIsAssignedOnlyToCorrectStrategy(csm, reader);

                // Remove SSTable and check that it will be removed from the correct compaction strategy
                csm.handleNotification(new SSTableDeletingNotification(reader), this);
                assertFalse(((SizeTieredCompactionStrategy)csm.getCompactionStrategyFor(reader)).sstables.contains(reader));

                // Add SSTable again and check that is correctly assigned
                csm.handleNotification(new SSTableAddedNotification(Collections.singleton(reader)), this);
                verifySSTableIsAssignedOnlyToCorrectStrategy(csm, reader);
            }
        }
    }

    private MockCFS createJBODMockCFS(int disks)
    {
        // Create #disks data directories to simulate JBOD
        Directories.DataDirectory[] directories = new Directories.DataDirectory[disks];
        for (int i = 0; i < disks; ++i)
        {
            File tempDir = Files.createTempDir();
            tempDir.deleteOnExit();
            directories[i] = new Directories.DataDirectory(tempDir);
        }

        ColumnFamilyStore cfs = Keyspace.open(KS_PREFIX).getColumnFamilyStore(TABLE_PREFIX);
        MockCFS mockCFS = new MockCFS(cfs, new Directories(cfs.metadata, directories));
        mockCFS.disableAutoCompaction();
        mockCFS.addSSTables(cfs.getLiveSSTables());
        return mockCFS;
    }

    /**
     * Updates the boundaries with a delta
     */
    private void updateBoundaries(MockBoundaryManager boundaryManager, int delta)
    {
        Integer[] oldBoundaries = boundaryManager.positions;
        Integer[] newBoundaries = Arrays.copyOf(oldBoundaries, oldBoundaries.length);

        for (int j = 0; j < newBoundaries.length - 1; j++)
        {
            if ((j + delta) % 2 == 0)
                newBoundaries[j] -= delta;
            else
                newBoundaries[j] += delta;
        }

        System.out.println("Old boundaries: " + Arrays.toString(oldBoundaries) +
                           " New boundaries: " + Arrays.toString(newBoundaries));

        // Check that SSTables are assigned to correct disks before change
        for (SSTableReader reader : boundaryManager.cfs.getLiveSSTables())
        {
            assertEquals(getSSTableIndex(oldBoundaries, reader), boundaryManager.getBoundaries().getDiskIndex(reader));
        }

        boundaryManager.updateBoundaries(newBoundaries);

        // Check that SSTables are assigned to correct disks after change
        for (SSTableReader reader : boundaryManager.cfs.getLiveSSTables())
        {
            assertEquals(getSSTableIndex(newBoundaries, reader), boundaryManager.getBoundaries().getDiskIndex(reader));
        }
    }

    private void verifySSTableIsAssignedOnlyToCorrectStrategy(CompactionStrategyManager csm, SSTableReader reader)
    {
        for (AbstractCompactionStrategy strategy : csm.getStrategies().stream().flatMap(s -> s.stream()).collect(Collectors.toSet()))
        {
            AbstractCompactionStrategy correctStrategy = csm.getCompactionStrategyFor(reader);
            SizeTieredCompactionStrategy stcs = (SizeTieredCompactionStrategy)strategy;
            if (stcs.sstables.contains(reader))
            {
                assertEquals(correctStrategy, stcs);
            }
            else
            {
                assertFalse(correctStrategy.equals(stcs));
            }
        }
    }

    /**
     * Since each SSTable contains keys from 0-99, and each sstable
     * generation is numbered from 1-100, since we are using ByteOrderedPartitioner
     * we can compute the sstable position in the disk boundaries by finding
     * the generation position relative to the boundaries
     */
    private int getSSTableIndex(Integer[] boundaries, SSTableReader reader)
    {
        int index = 0;
        while (boundaries[index] < reader.descriptor.generation)
            index++;
        System.out.println("Index for SSTable " + reader.descriptor.generation + " on boundary " + Arrays.toString(boundaries) + " is " + index);
        return index;
    }



    static class MockBoundaryManager
    {
        private final ColumnFamilyStore cfs;
        private Integer[] positions;
        private DiskBoundaries boundaries;

        public MockBoundaryManager(ColumnFamilyStore cfs, int numSSTables, int numDisks)
        {
            this.cfs = cfs;
            this.positions = computeInitialBoundaries(numSSTables, numDisks);
            this.boundaries = createDiskBoundaries(cfs, positions);
            System.out.println("Boundaries for " + numDisks + " disks is " + Arrays.toString(positions));
        }

        /**
         * Creates disk boundaries such that each disk receives
         * an equal amount of SSTables
         */
        private static Integer[] computeInitialBoundaries(int numSSTables, int numDisks)
        {
            Integer[] result = new Integer[numDisks];
            int sstablesPerRange = numSSTables / numDisks;
            result[0] = sstablesPerRange;
            for (int i = 1; i < numDisks; i++)
            {
                result[i] = result[i - 1] + sstablesPerRange;
            }
            result[numDisks - 1] = numSSTables; // make last boundary alwyays be the number of SSTables to prevent rounding errors
            return result;
        }

        public DiskBoundaries getBoundaries()
        {
            if (boundaries.isOutOfDate())
                boundaries = createDiskBoundaries(cfs, positions);
            return boundaries;
        }

        private DiskBoundaries createDiskBoundaries(ColumnFamilyStore cfs, Integer[] boundaries)
        {
            List<PartitionPosition> positions = Arrays.stream(boundaries).map(b -> Util.token(String.format(String.format("%04d", b))).minKeyBound()).collect(Collectors.toList());
            return new DiskBoundaries(cfs.getDirectories().getWriteableLocations(), positions, 0, 0);
        }

        public void updateBoundaries(Integer[] newBoundaries)
        {
            positions = newBoundaries;
            boundaries.invalidate();
        }
    }

    private static void createSSTableWithKey(String keyspace, String table, int key)
    {
        long timestamp = System.currentTimeMillis();
        DecoratedKey dk = Util.dk(String.format("%04d", key));
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        new RowUpdateBuilder(cfs.metadata, timestamp, dk.getKey())
        .clustering(Integer.toString(key))
        .add("val", "val")
        .build()
        .applyUnsafe();
        cfs.forceBlockingFlush();
    }

    // just to be able to override the data directories
    private static class MockCFS extends ColumnFamilyStore
    {
        MockCFS(ColumnFamilyStore cfs, Directories dirs)
        {
            super(cfs.keyspace, cfs.getTableName(), 0, cfs.metadata, dirs, false, false, true);
        }
    }
}
