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

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.DefaultFSErrorHandler;

import static org.apache.cassandra.db.DirectoriesTest.cfDir;
import static org.apache.cassandra.db.DirectoriesTest.createFakeSSTable;
import static org.apache.cassandra.schema.MockSchema.sstableId;
import static org.apache.cassandra.service.snapshot.TableSnapshotTest.createFolders;
import static org.apache.cassandra.utils.FBUtilities.now;
import static org.assertj.core.api.Assertions.assertThat;

public class SnapshotManagerTest
{
    static File fakeDataDir;

    private static final String KS = "ks";
    static long ONE_DAY_SECS = 86400;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        FileUtils.setFSErrorHandler(new DefaultFSErrorHandler());
        fakeDataDir = FileUtils.createTempFile("cassandra", "unittest");
        fakeDataDir.tryDelete(); // hack to create a temp dir
        fakeDataDir.tryCreateDirectory();
    }

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private TableSnapshot generateSnapshotDetails(String tag, Instant expiration) throws Exception {
        return new TableSnapshot(
        "ks",
        "tbl",
        UUID.randomUUID(),
        tag,
        Instant.EPOCH,
        expiration,
        createFolders(temporaryFolder));
    }

    @Test
    public void testAddSnapshots() throws Exception {
        TableSnapshot expired = generateSnapshotDetails("expired", Instant.EPOCH);
        TableSnapshot nonExpired = generateSnapshotDetails("non-expired", now().plusSeconds(ONE_DAY_SECS));
        TableSnapshot nonExpiring = generateSnapshotDetails("non-expiring", null);
        List<TableSnapshot> snapshots = Arrays.asList(expired, nonExpired, nonExpiring);

        // Create SnapshotManager with 3 snapshots: expired, non-expired and non-expiring
        SnapshotManager manager = new SnapshotManager(3, 3);
        manager.addSnapshots(snapshots);

        // Only expiring snapshots should be loaded
        assertThat(manager.getExpiringSnapshots()).hasSize(2);
        assertThat(manager.getExpiringSnapshots()).contains(expired);
        assertThat(manager.getExpiringSnapshots()).contains(nonExpired);
    }

    @Test
    public void testClearExpiredSnapshots() throws Exception {
        SnapshotManager manager = new SnapshotManager(3, 3);

        // Add 3 snapshots: expired, non-expired and non-expiring
        TableSnapshot expired = generateSnapshotDetails("expired", Instant.EPOCH);
        TableSnapshot nonExpired = generateSnapshotDetails("non-expired", now().plusMillis(ONE_DAY_SECS));
        TableSnapshot nonExpiring = generateSnapshotDetails("non-expiring", null);
        manager.addSnapshot(expired);
        manager.addSnapshot(nonExpired);
        manager.addSnapshot(nonExpiring);

        // Only expiring snapshot should be indexed and all should exist
        assertThat(manager.getExpiringSnapshots()).hasSize(2);
        assertThat(manager.getExpiringSnapshots()).contains(expired);
        assertThat(manager.getExpiringSnapshots()).contains(nonExpired);
        assertThat(expired.exists()).isTrue();
        assertThat(nonExpired.exists()).isTrue();
        assertThat(nonExpiring.exists()).isTrue();

        // After clearing expired snapshots, expired snapshot should be removed while the others should remain
        manager.clearExpiredSnapshots();
        assertThat(manager.getExpiringSnapshots()).hasSize(1);
        assertThat(manager.getExpiringSnapshots()).contains(nonExpired);
        assertThat(expired.exists()).isFalse();
        assertThat(nonExpired.exists()).isTrue();
        assertThat(nonExpiring.exists()).isTrue();
    }

    @Test
    public void testScheduledCleanup() throws Exception {
        SnapshotManager manager = new SnapshotManager(0, 1);
        try
        {
            // Start snapshot manager which should start expired snapshot cleanup thread
            manager.start();

            // Add 2 expiring snapshots: one to expire in 2 seconds, another in 1 day
            int TTL_SECS = 2;
            TableSnapshot toExpire = generateSnapshotDetails("to-expire", now().plusSeconds(TTL_SECS));
            TableSnapshot nonExpired = generateSnapshotDetails("non-expired", now().plusMillis(ONE_DAY_SECS));
            manager.addSnapshot(toExpire);
            manager.addSnapshot(nonExpired);

            // Check both snapshots still exist
            assertThat(toExpire.exists()).isTrue();
            assertThat(nonExpired.exists()).isTrue();
            assertThat(manager.getExpiringSnapshots()).hasSize(2);
            assertThat(manager.getExpiringSnapshots()).contains(toExpire);
            assertThat(manager.getExpiringSnapshots()).contains(nonExpired);

            // Sleep 4 seconds
            Thread.sleep((TTL_SECS + 2) * 1000L);

            // Snapshot with ttl=2s should be gone, while other should remain
            assertThat(manager.getExpiringSnapshots()).hasSize(1);
            assertThat(manager.getExpiringSnapshots()).contains(nonExpired);
            assertThat(toExpire.exists()).isFalse();
            assertThat(nonExpired.exists()).isTrue();
        }
        finally
        {
            manager.stop();
        }
    }

    @Test
    public void testClearSnapshot() throws Exception
    {
        // Given
        SnapshotManager manager = new SnapshotManager(1, 3);
        TableSnapshot expiringSnapshot = generateSnapshotDetails("snapshot", now().plusMillis(50000));
        manager.addSnapshot(expiringSnapshot);
        assertThat(manager.getExpiringSnapshots()).contains(expiringSnapshot);
        assertThat(expiringSnapshot.exists()).isTrue();

        // When
        manager.clearSnapshot(expiringSnapshot);

        // Then
        assertThat(manager.getExpiringSnapshots()).doesNotContain(expiringSnapshot);
        assertThat(expiringSnapshot.exists()).isFalse();
    }

    public static final String SNAPSHOT1 = "snapshot1";
    public static final String SNAPSHOT2 = "snapshot2";

    @Test
    public void testLoadSnapshotsFromDisk() throws Exception {
        SnapshotManager manager = new SnapshotManager();

        // Initial state
        TableMetadata fakeTable = createFakeTable("table");
        assertThat(manager.getSnapshots(fakeTable.id.asUUID())).isEmpty();

        // Create snapshot with and without manifest
        FakeSnapshot snapshot1 = createFakeSnapshot(fakeTable, SNAPSHOT1, true);
        FakeSnapshot snapshot2 = createFakeSnapshot(fakeTable, SNAPSHOT2, false);

        manager.loadSnapshots(new SnapshotLoader(fakeDataDir.absolutePath()));

        // Both snapshots should be present
        Set<TableSnapshot> snapshots = manager.getSnapshots(fakeTable.id.asUUID());
        assertThat(snapshots).hasSize(2);
        assertThat(snapshots).containsExactlyInAnyOrder(snapshot1.asTableSnapshot(), snapshot2.asTableSnapshot());

        // Now remove snapshot1 from manager
        assertThat(snapshot1.snapshotDir.exists()).isTrue();
        manager.clearSnapshot(snapshot1.asTableSnapshot());
        assertThat(snapshot1.snapshotDir.exists()).isFalse();

        // Only snapshot 2 should be present
        snapshots = manager.getSnapshots(fakeTable.id.asUUID());
        assertThat(snapshots).containsExactly(snapshot2.asTableSnapshot());
    }

    private TableMetadata createFakeTable(String table)
    {
        return TableMetadata.builder(KS, table)
                            .addPartitionKeyColumn("thekey", UTF8Type.instance)
                            .addClusteringColumn("thecolumn", UTF8Type.instance)
                            .build();
    }

    public FakeSnapshot createFakeSnapshot(TableMetadata table, String tag, boolean createManifest) throws IOException
    {
        File tableDir = cfDir(fakeDataDir, table);
        tableDir.tryCreateDirectories();
        File snapshotDir = new File(tableDir, Directories.SNAPSHOT_SUBDIR + File.pathSeparator() + tag);
        snapshotDir.tryCreateDirectories();

        Descriptor sstableDesc = new Descriptor(snapshotDir, KS, table.name, sstableId(1), SSTableFormat.Type.BIG);
        createFakeSSTable(sstableDesc);

        SnapshotManifest manifest = null;
        if (createManifest)
        {
            File manifestFile = Directories.getSnapshotManifestFile(snapshotDir);
            manifest = new SnapshotManifest(Collections.singletonList(sstableDesc.filenameFor(Component.DATA)), new DurationSpec("1m"), now());
            manifest.serializeToJsonFile(manifestFile);
        }

        return new FakeSnapshot(table, tag, snapshotDir, manifest);
    }

    class FakeSnapshot {
        final TableMetadata table;
        final String tag;
        final File snapshotDir;
        final SnapshotManifest manifest;

        FakeSnapshot(TableMetadata table, String tag, File snapshotDir, SnapshotManifest manifest)
        {
            this.table = table;
            this.tag = tag;
            this.snapshotDir = snapshotDir;
            this.manifest = manifest;
        }

        public TableSnapshot asTableSnapshot()
        {
            Instant createdAt = manifest == null ? null : manifest.createdAt;
            Instant expiresAt = manifest == null ? null : manifest.expiresAt;
            return new TableSnapshot(table.keyspace, table.name, table.id.asUUID(), tag, createdAt, expiresAt, Collections.singleton(snapshotDir));
        }
    }
}
