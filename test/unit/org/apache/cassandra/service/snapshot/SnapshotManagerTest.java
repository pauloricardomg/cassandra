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

package org.apache.cassandra.service.snapshot;

import java.util.Collection;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SnapshotManagerTest
{
    static final String KEYSPACE = "KEYSPACE";

    static int NUM_SSTABLES = 10;
    static int NUM_TABLES = 10;
    static int NUM_SNAPSHOTS = 100;

    static int NUM_RUNS = 1000;

    static SnapshotManager snapshotManager;

    @BeforeClass
    public static void beforeClass()
    {
        SchemaLoader.prepareServer();

        // Create Schema
        TableMetadata[] tables = new TableMetadata[NUM_TABLES];
        for (int i = 0; i < NUM_TABLES; i++)
        {
            tables[i] = SchemaLoader.standardCFMD(KEYSPACE, tableName(i)).build();
        }
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    tables);

        for (int i = 0; i < NUM_TABLES; i++)
        {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName(i));
            cfs.disableAutoCompaction();
            for (int j = 0; j < NUM_SSTABLES; j++)
            {
                System.out.printf("Creating sstable %d of table %s%n", j, i);
                new RowUpdateBuilder(cfs.metadata(), 0, "key1")
                .clustering("Column1")
                .add("val", "asdf")
                .build()
                .applyUnsafe();
                Util.flush(cfs);
            }
            for (int j = 0; j < NUM_SNAPSHOTS; j++)
            {
                System.out.printf("Creating snapshot %d of table %s%n", j, i);
                cfs.snapshot(snapshotName(j));
            }
        }
        snapshotManager = new SnapshotManager(0L, 0L);
        assertEquals(snapshotManager.getSnapshots(KEYSPACE).size(), 0);
        snapshotManager.addSnapshots(snapshotManager.loadSnapshots());
        assertEquals(snapshotManager.getSnapshots(KEYSPACE).size(), NUM_TABLES * NUM_SNAPSHOTS);
    }

    private static String tableName(int i)
    {
        return String.format("table%d", i);
    }

    private static String snapshotName(int i)
    {
        return String.format("snap%d", i);
    }

    @Test
    public void testListSnapshotsCached()
    {
        for (int i = 0; i < NUM_RUNS; i++)
        {
            assertEquals(listAllSnapshots(true, false).size(), NUM_TABLES * NUM_SNAPSHOTS);
        }
    }

    @Test
    public void testListSnapshotsCachedCheckExists()
    {
        for (int i = 0; i < NUM_RUNS; i++)
        {
            assertEquals(listAllSnapshots(true, true).size(), NUM_TABLES * NUM_SNAPSHOTS);
        }
    }

    @Test
    public void testListSnapshotsUncached()
    {
        for (int i = 0; i < NUM_RUNS; i++)
        {
            assertEquals(listAllSnapshots(false, false).size(), NUM_TABLES*NUM_SNAPSHOTS);
        }
    }

    private static Collection<TableSnapshot> listAllSnapshots(boolean cached, boolean checkExists)
    {
        Collection<TableSnapshot> result;
        if (cached)
        {
            result = snapshotManager.getSnapshots(KEYSPACE);
        }
        else
        {
            result = snapshotManager.loadSnapshots();
        }

        if (checkExists)
        {
            for (TableSnapshot snapshot : result)
            {
                for (File manifest : snapshot.getManifestFiles())
                {
                    assertTrue(manifest.exists());
                }
            }
        }

        return result;
    }
}
