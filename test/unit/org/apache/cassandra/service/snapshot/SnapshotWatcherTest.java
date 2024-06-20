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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.DefaultFSErrorHandler;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.service.snapshot.SnapshotManager.getSnapshotDirs;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SnapshotWatcherTest
{
    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @ClassRule
    public static TemporaryFolder temporaryFolder2 = new TemporaryFolder();

    private static File rootDir1;
    private static File rootDir2;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        FileUtils.setFSErrorHandler(new DefaultFSErrorHandler());
        rootDir1 = new File(temporaryFolder.getRoot());
        rootDir2 = new File(temporaryFolder2.getRoot());
    }

    @Test
    public void testWatcher() throws IOException, InterruptedException
    {
        SnapshotManager snapshotManager = new SnapshotManager(5, 10);
        SnapshotWatcher watcher = snapshotManager.getSnapshotWatcher();

        snapshotManager.start();
        snapshotManager.resumeSnapshotCleanup();
        assertTrue(watcher.isStarted());

        List<TableSnapshot> tableSnapshots = generateTableSnapshots(10, 100);

        snapshotManager.addSnapshots(tableSnapshots);

        Set<Path> watchedDirs = Set.of(Paths.get(rootDir1.absolutePath(), "ks", "tb", "snapshots"),
                                       Paths.get(rootDir2.absolutePath(), "ks", "tb", "snapshots"));

        // it watches just 2 dirs, the snapshot dir for each root
        assertWatchedDirs(watcher.getWatchedDirs(), 2, watchedDirs);

        // removal of a file in a snapshot does not unwatch it, nor it removes a snapshot itself
        // this one has root dir in 1st root dir
        removeFileInSnapshot(tableSnapshots.get(0));
        Thread.sleep(1000); // give watcher the chance to act on it (or not)
        assertWatchedDirs(watcher.getWatchedDirs(), 2, watchedDirs);
        assertEquals(0, snapshotManager.getSnapshotDirsForRemoval().size());

        // when directory of a snapshot is manually removed from disk, it will be detected by SnapshotWatcher, and
        // it will be eventually removed from SnapshotManager
        snapshotManager.pauseSnapshotCleanup();

        removeDirectoryOfSnapshot(tableSnapshots.get(0));
        Thread.sleep(1000); // give watcher the chance to act on it (or not)
        assertWatchedDirs(watcher.getWatchedDirs(), 2, watchedDirs);
        List<Path> snapshotDirsForRemoval = snapshotManager.getSnapshotDirsForRemoval();
        assertEquals(1, snapshotDirsForRemoval.size());

        snapshotManager.resumeSnapshotCleanup();

        await()
        .atMost(1, MINUTES)
        .pollInterval(5, SECONDS)
        .until(() -> {
            boolean removed = snapshotManager.getSnapshotDirsForRemoval().isEmpty();
            // we created 2000 of them
            boolean remainingSnapshots = 1999 == snapshotManager.getSnapshots(false, true).size();
            return removed && remainingSnapshots;
        });

        // we still watch 2 snapshot dirs, one for each root
        assertWatchedDirs(watcher.getWatchedDirs(), 2, watchedDirs);

        // when whole snapshot directory is removed, such directory is unwatched and all snapshots in it
        // which no longer exists (because they were manually removed from disk) will be removed from SnapshotManager

        removeSnapshotsDir(rootDir1, "ks", "tb");

        await()
        .atMost(1, MINUTES)
        .pollInterval(5, SECONDS)
        .until(() -> {
            boolean allSnapshotsToBeRemovedWereRemoved = snapshotManager.getSnapshotDirsForRemoval().isEmpty();
            // we removed whole root1
            boolean remainingSnapshots = 1000 == snapshotManager.getSnapshots(false, true).size();
            boolean jusOneDirToWatchRemains = watcher.getWatchedDirs().size() == 1;
            return allSnapshotsToBeRemovedWereRemoved && remainingSnapshots && jusOneDirToWatchRemains;
        });

        // we removed all snapshots and snapshot dir itself so just snapshots in root2 remain

        watchedDirs = Set.of(Paths.get(rootDir2.absolutePath(), "ks", "tb", "snapshots"));
        assertWatchedDirs(watcher.getWatchedDirs(), 1, watchedDirs);

        // remove remaining snapshots "the normal way"
        for (TableSnapshot tableSnapshot : tableSnapshots)
            snapshotManager.clearSnapshot(tableSnapshot);

        await()
        .atMost(1, MINUTES)
        .pollInterval(5, SECONDS)
        .until(() -> {
            boolean allSnapshotsToBeRemovedWereRemoved = snapshotManager.getSnapshotDirsForRemoval().isEmpty();
            // we removed whole root2
            boolean remainingSnapshots = snapshotManager.getSnapshots(false, true).isEmpty();
            boolean jusOneDirToWatchRemains = watcher.getWatchedDirs().isEmpty();
            return allSnapshotsToBeRemovedWereRemoved && remainingSnapshots && jusOneDirToWatchRemains;
        });

        snapshotManager.stop();
    }

    private List<TableSnapshot> generateTableSnapshots(int keyspaces, int tables) throws IOException
    {
        List<TableSnapshot> tableSnapshots = new ArrayList<>();
        for (int i = 0; i < keyspaces; i++)
        {
            for (int j = 0; j < tables; j++)
            {
                String snapshotName = format("mysnapshot_%s_%s", i, j);
                TableSnapshot snapshot1 = generateSnapshotDetails(rootDir1, snapshotName, "ks", "tb", null, false);
                TableSnapshot snapshot2 = generateSnapshotDetails(rootDir2, snapshotName, "ks", "tb", null, false);
                generateFileInSnapshot(snapshot1);
                tableSnapshots.add(snapshot1);
                tableSnapshots.add(snapshot2);
            }
        }

        return tableSnapshots;
    }

    private void generateFileInSnapshot(TableSnapshot tableSnapshot) throws IOException
    {
        Files.createFile(tableSnapshot.getDirectories().iterator().next().toPath().resolve("aFile"));
    }

    private void removeFileInSnapshot(TableSnapshot tableSnapshot) throws IOException
    {
        Files.deleteIfExists(tableSnapshot.getDirectories().iterator().next().toPath().resolve("aFile"));
    }

    private void removeDirectoryOfSnapshot(TableSnapshot tableSnapshot)
    {
        File snapshotDir = new File(tableSnapshot.getDirectories().iterator().next().toPath());
        snapshotDir.deleteRecursive();
    }

    private void removeSnapshotsDir(File rootDir, String keyspace, String table)
    {
        File snapshotsDir = new File(rootDir.toJavaIOFile().toPath().resolve(keyspace).resolve(table).resolve("snapshots"));
        snapshotsDir.deleteRecursive();
    }

    private void assertWatchedDirs(Collection<Path> watchedDirs, int count, Set<Path> expectedWatchedDirs)
    {
        assertNotNull(watchedDirs);
        assertNotNull(expectedWatchedDirs);
        assertEquals(count, watchedDirs.size());
        assertEquals(count, expectedWatchedDirs.size());

        for (Path expectedWatcheDir : expectedWatchedDirs)
            assertTrue(watchedDirs.contains(expectedWatcheDir));
    }

    private TableSnapshot generateSnapshotDetails(File root,
                                                  String tag,
                                                  String keyspace,
                                                  String table,
                                                  Instant expiration,
                                                  boolean ephemeral)
    {
        try
        {
            return new TableSnapshot(keyspace,
                                     table,
                                     UUID.randomUUID(),
                                     tag,
                                     Instant.EPOCH,
                                     expiration,
                                     getSnapshotDirs(tag, Set.of(createDir(root, keyspace, table))),
                                     ephemeral);
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    public static File createDir(File root, String first, String... more)
    {
        File file = new File(root, first);
        for (int i = 0; i < more.length; i++)
            file = new File(file, more[i]);

        file.tryCreateDirectories();
        return file;
    }
}
