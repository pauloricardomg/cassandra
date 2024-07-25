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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.service.DefaultFSErrorHandler;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
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

    @After
    public void afterTest()
    {
        PathUtils.clearDirectory(rootDir1.toPath());
        PathUtils.clearDirectory(rootDir2.toPath());
    }

    @Test
    public void testDisabledWatcher() throws Exception
    {
        try
        {
            DatabaseDescriptor.setSnapshotWatcherEnabled(false);

            SnapshotManager snapshotManager = new SnapshotManager(5, 10);
            SnapshotWatcher watcher = snapshotManager.getSnapshotWatcher();

            snapshotManager.start(true);

            List<TableSnapshot> tableSnapshots = generateTableSnapshots(10, 100);
            snapshotManager.addSnapshots(tableSnapshots);

            assertTrue(watcher.getWatchedDirs().isEmpty());

            snapshotManager.stop();
        }
        finally
        {
            DatabaseDescriptor.setSnapshotWatcherEnabled(true);
        }
    }

    @Test
    public void testWatcher() throws Exception
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

        // when directory of a snapshot is manually removed from disk, it will be detected by SnapshotWatcher
        // but such snapshot is not removed because there is still the second data dir present
        snapshotManager.pauseSnapshotCleanup();

        removeDirectoryOfSnapshot(tableSnapshots.get(0));
        Thread.sleep(1000); // give watcher the chance to act on it (or not)
        assertWatchedDirs(watcher.getWatchedDirs(), 2, watchedDirs);
        List<Path> snapshotDirsForRemoval = snapshotManager.getSnapshotDirsForRemoval();
        assertEquals(1, snapshotDirsForRemoval.size());

        snapshotManager.resumeSnapshotCleanup();

        // we created 1000 snapshots, even we removed one of data dirs of a snapshot, it was not removed from tracking
        waitOnNumberOfSnapshots(snapshotManager, watcher, 1000, 2);
        // we still watch 2 snapshot dirs, one for each root
        assertWatchedDirs(watcher.getWatchedDirs(), 2, watchedDirs);

        // remove the second data dir of the snapshot, this will remove such snapshot from tracking
        // because there are no datadirs of such snapshot anymore
        snapshotManager.pauseSnapshotCleanup();

        removeDirectoryOfSnapshot(tableSnapshots.get(0));
        assertWatchedDirs(watcher.getWatchedDirs(), 2, watchedDirs);
        snapshotDirsForRemoval = snapshotManager.getSnapshotDirsForRemoval();
        assertEquals(1, snapshotDirsForRemoval.size());

        snapshotManager.resumeSnapshotCleanup();

        // removed second data dir results in untracking
        waitOnNumberOfSnapshots(snapshotManager, watcher, 999, 2);

        // when whole snapshot directory is removed, such directory is unwatched and all snapshots in it
        // which no longer exists (because they were manually removed from disk) will be removed from SnapshotManager
        // here, all snapshots are spread over two data dirs so the fact we removed the first root dir does not
        // untrack any snapshots because there is still the second "half" of it.

        removeSnapshotsDir(rootDir1, "ks", "tb");
        waitOnNumberOfSnapshots(snapshotManager, watcher, 999, 1);
        watchedDirs = Set.of(Paths.get(rootDir2.absolutePath(), "ks", "tb", "snapshots"));
        assertWatchedDirs(watcher.getWatchedDirs(), 1, watchedDirs);

        // removal of the second snapshot dir results in unwatching and untracking everything
        removeSnapshotsDir(rootDir2, "ks", "tb");
        waitOnNumberOfSnapshots(snapshotManager, watcher, 0, 0);

        snapshotManager.stop();
    }

    private void waitOnNumberOfSnapshots(SnapshotManager snapshotManager,
                                         SnapshotWatcher watcher,
                                         int snapshotsNumber,
                                         int dirsWatched)
    {
        await()
        .atMost(1, MINUTES)
        .pollInterval(5, SECONDS)
        .until(() -> {
            boolean removed = snapshotManager.getSnapshotDirsForRemoval().isEmpty();
            boolean remainingSnapshots = snapshotsNumber == snapshotManager.getSnapshots(false, true).size();
            boolean dirsRemainedToBeWatched = watcher.getWatchedDirs().size() == dirsWatched;
            return removed && remainingSnapshots && dirsRemainedToBeWatched;
        });
    }

    private List<TableSnapshot> generateTableSnapshots(int keyspaces, int tables) throws IOException
    {
        List<TableSnapshot> tableSnapshots = new ArrayList<>();
        for (int i = 0; i < keyspaces; i++)
        {
            for (int j = 0; j < tables; j++)
            {
                String snapshotName = format("mysnapshot_%s_%s", i, j);
                File dir1 = new File(Paths.get(rootDir1.absolutePath(), "ks", "tb", "snapshots", snapshotName));
                File dir2 = new File(Paths.get(rootDir2.absolutePath(), "ks", "tb", "snapshots", snapshotName));
                dir1.tryCreateDirectories();
                dir2.tryCreateDirectories();
                TableSnapshot snapshot = generateSnapshotDetails(Set.of(dir1, dir2), snapshotName, "ks", "tb", null, false);
                generateFileInSnapshot(snapshot);
                tableSnapshots.add(snapshot);
            }
        }

        return tableSnapshots;
    }

    private void generateFileInSnapshot(TableSnapshot tableSnapshot) throws IOException
    {
        for (File snapshotDir : tableSnapshot.getDirectories())
            Files.createFile(snapshotDir.toPath().resolve("aFile"));
    }

    private void removeFileInSnapshot(TableSnapshot tableSnapshot) throws IOException
    {
        for (File snapshotDir : tableSnapshot.getDirectories())
            Files.deleteIfExists(snapshotDir.toPath().resolve("aFile"));
    }

    private void removeDirectoryOfSnapshot(TableSnapshot tableSnapshot)
    {
        for (File snapshotDir : tableSnapshot.getDirectories())
        {
            if (snapshotDir.exists())
            {
                snapshotDir.deleteRecursive();
                break;
            }
        }
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

    private TableSnapshot generateSnapshotDetails(Set<File> roots,
                                                  String tag,
                                                  String keyspace,
                                                  String table,
                                                  Instant expiration,
                                                  boolean ephemeral)
    {
        try
        {
            Set<File> snapshotDirs = new HashSet<>();
            for (File root : roots)
            {
                root.tryCreateDirectories();
                snapshotDirs.add(root);
            }

            return new TableSnapshot(keyspace,
                                     table,
                                     UUID.randomUUID(),
                                     tag,
                                     Instant.EPOCH,
                                     expiration,
                                     snapshotDirs,
                                     ephemeral);
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
