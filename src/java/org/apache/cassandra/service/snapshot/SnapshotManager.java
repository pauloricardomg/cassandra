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
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SchemaCQLHelper;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.DirectorySizeCalculator;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public class SnapshotManager implements AutoCloseable
{
    private static final ScheduledExecutorPlus executor = executorFactory().scheduled(false, "SnapshotCleanup");

    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

    public static final SnapshotManager instance = new SnapshotManager();

    private final long initialDelaySeconds;
    private final long cleanupPeriodSeconds;
    private final SnapshotLoader snapshotLoader;
    private final SnapshotWatcher snapshotWatcher;
    public final RateLimiter snapshotRateLimiter;

    @VisibleForTesting
    protected volatile ScheduledFuture<?> cleanupTaskFuture;
    protected volatile ScheduledFuture<?> manuallyRemovedSnapshotsTaskFuture;

    private final Set<TableSnapshot> liveSnapshots = Collections.synchronizedSet(new HashSet<>());

    /**
     * Expiring snapshots ordered by expiration date, to allow only iterating over snapshots
     * that need to be removed
     */
    private final PriorityBlockingQueue<TableSnapshot> expiringSnapshots = new PriorityBlockingQueue<>(10, comparing(TableSnapshot::getExpiresAt));
    private final PriorityBlockingQueue<Path> removedSnapshots = new PriorityBlockingQueue<>();

    private SnapshotManager()
    {
        this(CassandraRelevantProperties.SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS.getInt(),
             CassandraRelevantProperties.SNAPSHOT_CLEANUP_PERIOD_SECONDS.getInt());
    }

    @VisibleForTesting
    protected SnapshotManager(long initialDelaySeconds, long cleanupPeriodSeconds)
    {
        this(initialDelaySeconds, cleanupPeriodSeconds, DatabaseDescriptor.getAllDataFileLocations());
    }

    @VisibleForTesting
    protected SnapshotManager(long initialDelaySeconds, long cleanupPeriodSeconds, String[] dataDirs)
    {
        this.initialDelaySeconds = initialDelaySeconds;
        this.cleanupPeriodSeconds = cleanupPeriodSeconds;
        snapshotLoader = new SnapshotLoader(dataDirs);
        snapshotWatcher = new SnapshotWatcher(removedSnapshots::add);
        snapshotRateLimiter = DatabaseDescriptor.getSnapshotRateLimiter();
    }

    public static void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownNowAndWait(timeout, unit, executor);
    }

    public synchronized void start(boolean runPeriodicSnapshotCleaner)
    {
        snapshotWatcher.start();
        addSnapshots(loadSnapshots());
        if (runPeriodicSnapshotCleaner)
            resumeSnapshotCleanup();
    }

    public synchronized void start()
    {
        start(false);
    }

    @Override
    public synchronized void close()
    {
        pauseSnapshotCleanup();
        snapshotWatcher.close();
        expiringSnapshots.clear();
        liveSnapshots.clear();
    }

    @VisibleForTesting
    public SnapshotWatcher getSnapshotWatcher()
    {
        return snapshotWatcher;
    }

    public synchronized Set<TableSnapshot> loadSnapshots()
    {
        return snapshotLoader.loadSnapshots();
    }

    public synchronized void restart()
    {
        close();
        start(false);
    }

    public synchronized void restart(boolean runPeriodicSnapshotCleaner)
    {
        close();
        start(runPeriodicSnapshotCleaner);
    }

    public synchronized void addSnapshot(TableSnapshot snapshot)
    {
        logger.debug("Adding snapshot {}", snapshot);

        if (snapshot.isExpiring())
            expiringSnapshots.add(snapshot);
        else
            liveSnapshots.add(snapshot);

        snapshotWatcher.watch(snapshot);
    }

    @VisibleForTesting
    protected synchronized void addSnapshots(Collection<TableSnapshot> snapshots)
    {
        snapshots.forEach(this::addSnapshot);
    }

    @VisibleForTesting
    public synchronized void resumeSnapshotCleanup()
    {
        if (cleanupTaskFuture == null)
        {
            logger.info("Scheduling expired snapshots cleanup with initialDelaySeconds={} and cleanupPeriodSeconds={}",
                        initialDelaySeconds, cleanupPeriodSeconds);

            cleanupTaskFuture = executor.scheduleWithFixedDelay(this::clearExpiredSnapshots,
                                                                initialDelaySeconds,
                                                                cleanupPeriodSeconds,
                                                                SECONDS);
        }

        if (manuallyRemovedSnapshotsTaskFuture == null && DatabaseDescriptor.isSnapshotWatcherEnabled())
        {
            manuallyRemovedSnapshotsTaskFuture = executor.scheduleWithFixedDelay(this::cleanupManuallyRemovedSnapshots,
                                                                                 initialDelaySeconds,
                                                                                 cleanupPeriodSeconds,
                                                                                 SECONDS);
        }
    }

    @VisibleForTesting
    synchronized void pauseSnapshotCleanup()
    {
        if (cleanupTaskFuture != null)
        {
            cleanupTaskFuture.cancel(false);
            cleanupTaskFuture = null;
        }

        if (manuallyRemovedSnapshotsTaskFuture != null && DatabaseDescriptor.isSnapshotWatcherEnabled())
        {
            manuallyRemovedSnapshotsTaskFuture.cancel(false);
            manuallyRemovedSnapshotsTaskFuture = null;
        }
    }

    /**
     * Clears manually removed snapshots (when a user removes
     * a snapshot directory from the disk)
     */
    public synchronized void cleanupManuallyRemovedSnapshots()
    {
        Path removedSnapshot;
        while ((removedSnapshot = removedSnapshots.peek()) != null)
        {
            removedSnapshots.remove(removedSnapshot);
            Optional<TableSnapshot> maybeRemovedSnapshot = getSnapshot(removedSnapshot);

            // Remove the snapshot from tracking in memory.
            // If all its data dirs are removed and a snapshot is spread across 3 data dirs,
            // then removing one data dir will not automatically remove the rest.
            // The snapshot will be removed from tracking only if no data dir of such snapshot exists.
            if (maybeRemovedSnapshot.isPresent())
            {
                TableSnapshot tableSnapshot = maybeRemovedSnapshot.get();
                boolean allDirsRemoved = true;
                for (File snapshotDir : tableSnapshot.getDirectories())
                {
                    if (snapshotDir.exists())
                    {
                        allDirsRemoved = false;
                        break;
                    }
                }

                if (allDirsRemoved)
                {
                    // just in case, be extra careful to not remove anything, just untrack
                    clearSnapshot(tableSnapshot, false);
                }
            }

            Path rootSnapshotDirPath = removedSnapshot.getParent();
            File rootSnapshotDirFile = new File(rootSnapshotDirPath);
            File[] subdirs = rootSnapshotDirFile.tryList();
            if (subdirs == null || subdirs.length == 0)
                snapshotWatcher.unwatch(rootSnapshotDirPath);
        }
    }

    /**
     * Deletes snapshot and removes it from manager.
     *
     * @param snapshot snapshot to clear
     */
    public synchronized void clearSnapshot(TableSnapshot snapshot)
    {
        clearSnapshot(snapshot, true);
    }

    /**
     * Deletes snapshot and removes it from manager. It is possible to keep data
     * on disk and just untracking the snapshot from manager by setting {@code deleteData} to {@code false}.
     *
     * @param snapshot snapshot to delete
     * @param deleteData if true, data will be deleted, otherwise not
     */
    public synchronized void clearSnapshot(TableSnapshot snapshot, boolean deleteData)
    {
        logger.debug("Removing snapshot {}, {}deleting data", snapshot, (deleteData ? "" : "not "));

        if (deleteData)
        {
            for (File snapshotDir : snapshot.getDirectories())
            {
                try
                {
                    removeSnapshotDirectory(snapshotDir);
                }
                catch (Exception ex)
                {
                    logger.warn("Unable to remove snapshot directory {}", snapshotDir, ex);
                }
            }
        }

        if (snapshot.isExpiring())
            expiringSnapshots.remove(snapshot);
        else
            liveSnapshots.remove(snapshot);
    }

    /**
     * Removes all snapshots, expiring and non-expiring ones.
     */
    public synchronized void clearAllSnapshots()
    {
        clearAllSnapshotsInternal(liveSnapshots.iterator());
        clearAllSnapshotsInternal(expiringSnapshots.iterator());
    }

    private void clearAllSnapshotsInternal(Iterator<TableSnapshot> iterator)
    {
        while (iterator.hasNext())
        {
            TableSnapshot next = iterator.next();
            for (File snapshotDir : next.getDirectories())
            {
                try
                {
                    removeSnapshotDirectory(snapshotDir);
                }
                catch (Exception ex)
                {
                    logger.warn("Unable to remove snapshot directory {}", snapshotDir, ex);
                }
            }

            iterator.remove();
        }
    }

    /**
     * Finds a snapshot by a path. A path would be equal to one of snapshots directories.
     *
     * @param snapshotPath path to snapshot to find logical snapshot for
     * @return optional representation of a snapshot
     */
    private Optional<TableSnapshot> getSnapshot(Path snapshotPath)
    {
        List<TableSnapshot> snapshots = getSnapshots(snapshot -> {
            for (File snapshotFile : snapshot.getDirectories())
                if (snapshotFile.toPath().equals(snapshotPath))
                    return true;

            return false;
        });

        return snapshots.isEmpty() ? Optional.empty() : Optional.of(snapshots.get(0));
    }

    /**
     * Returns list of snapshots of given keyspace
     *
     * @param keyspace keyspace of a snapshot
     * @return list of snapshots of given keyspace.
     */
    public List<TableSnapshot> getSnapshots(String keyspace)
    {
        return getSnapshots(snapshot -> snapshot.getKeyspaceName().equals(keyspace));
    }

    /**
     * Returns list of snapshots from given keyspace and table.
     *
     * @param keyspace keyspace of a snapshot
     * @param table    table of a snapshot
     * @return list of snapshots from given keyspace and table
     */
    public List<TableSnapshot> getSnapshots(String keyspace, String table)
    {
        return getSnapshots(snapshot -> snapshot.getKeyspaceName().equals(keyspace) &&
                                        snapshot.getTableName().equals(table));
    }

    /**
     * Returns a snapshot or empty optional based on the given parameters.
     *
     * @param keyspace keyspace of a snapshot
     * @param table    table of a snapshot
     * @param tag      name of a snapshot
     * @return empty optional if there is not such snapshot, non-empty otherwise
     */
    public synchronized Optional<TableSnapshot> getSnapshot(String keyspace, String table, String tag)
    {
        // we do not use the predicate here because we want to stop the loop as soon as
        // we find the snapshot we are looking for, looping until the end is not necessary
        for (TableSnapshot snapshot : Iterables.concat(liveSnapshots, expiringSnapshots))
        {
            if (snapshot.getKeyspaceName().equals(keyspace) &&
                snapshot.getTableName().equals(table) &&
                snapshot.getTag().equals(tag) || (tag != null && tag.isEmpty()))
            {
                return Optional.of(snapshot);
            }
        }

        return Optional.empty();
    }

    /**
     * Return snapshots based on given parameters.
     *
     * @param skipExpiring     if expiring snapshots should be skipped
     * @param includeEphemeral if ephemeral snapshots should be included
     * @return snapshots based on given parameters
     */
    public List<TableSnapshot> getSnapshots(boolean skipExpiring, boolean includeEphemeral)
    {
        return getSnapshots(s -> (!skipExpiring || !s.isExpiring()) && (includeEphemeral || !s.isEphemeral()));
    }

    /**
     * @return all ephemeral snapshots in a node
     */
    public List<TableSnapshot> getEphemeralSnapshots()
    {
        return getSnapshots(TableSnapshot::isEphemeral);
    }

    /**
     * Returns all snapshots passing the given predicate.
     *
     * @param predicate predicate to filter all snapshots of
     * @return list of snapshots passing the predicate
     */
    public synchronized List<TableSnapshot> getSnapshots(Predicate<TableSnapshot> predicate)
    {
        List<TableSnapshot> snapshots = new ArrayList<>();
        for (TableSnapshot snapshot : Iterables.concat(liveSnapshots, expiringSnapshots))
            if (predicate.test(snapshot))
                snapshots.add(snapshot);

        return snapshots;
    }

    public Collection<TableSnapshot> getExpiringSnapshots()
    {
        return expiringSnapshots;
    }

    /**
     * Clear snapshots of given tag from given keyspaces.
     * <p>
     * If tag is not present / is empty, all snapshots are considered to be cleared.
     * If keyspaces are empty, all snapshots of given tag and older than maxCreatedAt are removed.
     * <p>
     * Ephemeral snapshots are not included.
     *
     * @param tag          optional tag of snapshot to clear
     * @param keyspaces    keyspaces to remove snapshots for
     * @param maxCreatedAt clear all such snapshots which were created before this timestamp
     */
    public void clearSnapshots(String tag, Set<String> keyspaces, long maxCreatedAt)
    {
        clearSnapshots(tag, keyspaces, maxCreatedAt, false);
    }

    /**
     * Clear snapshots of given tag from given keyspace.
     * <p>
     *
     * @param tag      snapshot name
     * @param keyspace keyspace to clear all snapshots of a given tag of
     */
    public void clearSnapshots(String tag, String keyspace)
    {
        clearSnapshots(tag, Set.of(keyspace), Clock.Global.currentTimeMillis(), false);
    }

    /**
     * Removes a snapshot.
     * <p>
     *
     * @param keyspace keyspace of a snapshot to remove
     * @param table    table of a snapshot to remove
     * @param tag      name of a snapshot to remove.
     */
    public void clearSnapshot(String keyspace, String table, String tag)
    {
        getSnapshot(keyspace, table, tag).ifPresent(this::clearSnapshot);
    }

    /**
     * Clears all ephemeral snapshots in a node.
     */
    public void clearEphemeralSnapshots()
    {
        getEphemeralSnapshots().forEach(this::clearSnapshot);
    }

    /**
     * Clears all expired snapshots in a node.
     */
    public synchronized void clearExpiredSnapshots()
    {
        Instant now = FBUtilities.now();
        getSnapshots(s -> s.isExpired(now)).forEach(this::clearSnapshot);
    }

    /**
     * Clear snapshots of given tag from given keyspaces.
     * <p>
     * If tag is not present / is empty, all snapshots are considered to be cleared.
     * If keyspaces are empty, all snapshots of given tag and older than maxCreatedAt are removed.
     *
     * @param tag              optional tag of snapshot to clear
     * @param keyspaces        keyspaces to remove snapshots for
     * @param maxCreatedAt     clear all such snapshots which were created before this timestamp
     * @param includeEphemeral include ephemeral snaphots for removal or not
     */
    public synchronized void clearSnapshots(String tag, Set<String> keyspaces,
                                            long maxCreatedAt,
                                            boolean includeEphemeral)
    {
        Predicate<TableSnapshot> predicate = shouldClearSnapshot(tag, keyspaces, maxCreatedAt, includeEphemeral);
        getSnapshots(predicate).forEach(this::clearSnapshot);
    }

    @VisibleForTesting
    List<Path> getSnapshotDirsForRemoval()
    {
        List<Path> removedSnapshots = new ArrayList<>();
        for (Object object : this.removedSnapshots.toArray())
            removedSnapshots.add((Path) object);

        return removedSnapshots;
    }

    /**
     * Takes a snapshot by creating hardlinks into snapshot directories. This method also
     * creates manifests and schema files and such snapshot will be added among tracked ones in this manager.
     *
     * @param cfs          column family to create a snapshot for
     * @param tag          name of snapshot
     * @param ephemeral    true if the snapshot is ephemeral, false otherwise
     * @param ttl          time after the created snapshot will be removed
     * @param creationTime time the snapshot was created
     * @param rateLimiter  limiter for hard-links creation, if null, limiter from DatabaseDescriptor will be used
     * @return logical representation of a snapshot
     */
    public TableSnapshot createSnapshot(ColumnFamilyStore cfs,
                                        String tag,
                                        com.google.common.base.Predicate<SSTableReader> predicate,
                                        boolean ephemeral,
                                        DurationSpec.IntSecondsBound ttl,
                                        Instant creationTime,
                                        RateLimiter rateLimiter)
    {
        if (ephemeral && ttl != null)
            throw new IllegalStateException(String.format("can not take ephemeral snapshot (%s) while ttl is specified too", tag));

        RateLimiter limiter = rateLimiter;
        if (limiter == null)
            limiter = SnapshotManager.instance.snapshotRateLimiter;

        Set<SSTableReader> sstables = new LinkedHashSet<>();
        for (ColumnFamilyStore aCfs : cfs.concatWithIndexes())
        {
            try (ColumnFamilyStore.RefViewFragment currentView = aCfs.selectAndReference(View.select(SSTableSet.CANONICAL, (x) -> predicate == null || predicate.apply(x))))
            {
                for (SSTableReader ssTable : currentView.sstables)
                {
                    File snapshotDirectory = Directories.getSnapshotDirectory(ssTable.descriptor, tag);
                    ssTable.createLinks(snapshotDirectory.path(), limiter); // hard links
                    if (logger.isTraceEnabled())
                        logger.trace("Snapshot for {} keyspace data file {} created in {}", cfs.keyspace, ssTable.getFilename(), snapshotDirectory);
                    sstables.add(ssTable);
                }
            }
        }

        Map<File, List<String>> snapshotDirSSTablesMap = new HashMap<>();
        for (File snapshotDir : cfs.getDirectories().getSnapshotDirs(tag))
            snapshotDirSSTablesMap.put(snapshotDir.toAbsolute(), new ArrayList<>());

        // categorize each sstable into respective snapshot dir
        for (SSTableReader sstable : sstables)
        {
            File snapshotDirOfSStable = Directories.getSnapshotDirectory(sstable.descriptor, tag);

            File d = null;
            if (snapshotDirSSTablesMap.get(snapshotDirOfSStable) != null)
                d = snapshotDirOfSStable;
            // if it is sstable from index
            else if (snapshotDirSSTablesMap.get(snapshotDirOfSStable.parent()) != null)
                d = snapshotDirOfSStable.parent();

            if (d != null)
            {
                List<String> sstablesInDir = snapshotDirSSTablesMap.get(d);
                if (sstablesInDir != null)
                    sstablesInDir.add(sstable.descriptor.relativeFilenameFor(SSTableFormat.Components.DATA));
            }
        }

        for (Map.Entry<File, List<String>> entry : snapshotDirSSTablesMap.entrySet())
        {
            File snapshotDir = entry.getKey();
            List<String> files = entry.getValue();
            SnapshotManifest manifest = new SnapshotManifest(files, ttl, creationTime, ephemeral);
            File manifestFile = Directories.getSnapshotManifestFile(snapshotDir);
            writeSnapshotManifest(manifest, manifestFile);

            if (!SchemaConstants.isLocalSystemKeyspace(cfs.metadata.keyspace)
                && !SchemaConstants.isReplicatedSystemKeyspace(cfs.metadata.keyspace))
            {
                File schemaFile = Directories.getSnapshotSchemaFile(snapshotDir);
                writeSnapshotSchema(schemaFile, cfs);
            }
        }

        TableSnapshot snapshot = new TableSnapshot(cfs.metadata.keyspace,
                                                   cfs.metadata.name,
                                                   cfs.metadata.id.asUUID(),
                                                   tag,
                                                   creationTime,
                                                   SnapshotManifest.computeExpiration(ttl, creationTime),
                                                   snapshotDirSSTablesMap.keySet(),
                                                   ephemeral);

        addSnapshot(snapshot);
        return snapshot;
    }

    /**
     * Returns a predicate based on which a snapshot will be included for deletion or not.
     *
     * @param tag                name of snapshot to remove
     * @param keyspaces          keyspaces this snapshot belongs to
     * @param olderThanTimestamp clear the snapshot if it is older than given timestamp
     * @param includeEphemeral   whether to include ephemeral snapshots as well
     * @return predicate which filters snapshots on given parameters
     */
    public static Predicate<TableSnapshot> shouldClearSnapshot(String tag,
                                                               Set<String> keyspaces,
                                                               long olderThanTimestamp,
                                                               boolean includeEphemeral)
    {
        return ts ->
        {
            // When no tag is supplied, all snapshots must be cleared
            boolean clearAll = tag == null || tag.isEmpty();
            if (!clearAll && ts.isEphemeral() && !includeEphemeral)
                logger.info("Skipping deletion of ephemeral snapshot '{}' in keyspace {}. " +
                            "Ephemeral snapshots are not removable by a user.",
                            tag, ts.getKeyspaceName());
            boolean passedEphemeralTest = !ts.isEphemeral() || (ts.isEphemeral() && includeEphemeral);
            boolean shouldClearTag = clearAll || ts.getTag().equals(tag);
            boolean byTimestamp = true;

            if (olderThanTimestamp > 0L)
            {
                Instant createdAt = ts.getCreatedAt();
                if (createdAt != null)
                    byTimestamp = createdAt.isBefore(Instant.ofEpochMilli(olderThanTimestamp));
            }

            boolean byKeyspace = (keyspaces.isEmpty() || keyspaces.contains(ts.getKeyspaceName()));

            return passedEphemeralTest && shouldClearTag && byTimestamp && byKeyspace;
        };
    }

    public long trueSnapshotsSize(TableMetadata tableMetadata, Directories directories)
    {
        long result = 0L;
        for (File dir : directories.getDataPaths())
        {
            File snapshotDir = Directories.isSecondaryIndexFolder(dir)
                               ? new File(dir.parentPath(), Directories.SNAPSHOT_SUBDIR)
                               : new File(dir, Directories.SNAPSHOT_SUBDIR);
            result += getTrueAllocatedSizeIn(tableMetadata, directories, snapshotDir);
        }
        return result;
    }

    private long getTrueAllocatedSizeIn(TableMetadata metadata, Directories directories, File snapshotDir)
    {
        if (!snapshotDir.isDirectory())
            return 0;

        SSTableSizeSummer visitor = new SSTableSizeSummer(metadata, directories
                                                                    .sstableLister(Directories.OnTxnErr.THROW)
                                                                    .listFiles());
        try
        {
            Files.walkFileTree(snapshotDir.toPath(), visitor);
        }
        catch (IOException e)
        {
            logger.error("Could not calculate the size of {}. {}", snapshotDir, e.getMessage());
        }

        return visitor.getAllocatedSize();
    }

    private static class SSTableSizeSummer extends DirectorySizeCalculator
    {
        private final Set<String> toSkip = new HashSet<>();
        private final TableMetadata metadata;

        private SSTableSizeSummer(TableMetadata metadata, List<File> files)
        {
            for (File file : files)
                toSkip.add(file.name());

            this.metadata = metadata;
        }

        @Override
        public boolean isAcceptable(Path path)
        {
            File file = new File(path);
            String fileName = file.name();

            if (fileName.equals("manifest.json") || fileName.equals("schema.cql"))
                return true;

            Descriptor desc = SSTable.tryDescriptorFromFile(file);
            return desc != null
                   && desc.ksname.equals(metadata.keyspace)
                   && desc.cfname.equals(metadata.name)
                   && !toSkip.contains(fileName);
        }
    }

    private void writeSnapshotManifest(SnapshotManifest manifest, File manifestFile)
    {
        try
        {
            manifestFile.parent().tryCreateDirectories();
            manifest.serializeToJsonFile(manifestFile);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, manifestFile);
        }
    }

    private void writeSnapshotSchema(File schemaFile, ColumnFamilyStore cfs)
    {
        try
        {
            if (!schemaFile.parent().exists())
                schemaFile.parent().tryCreateDirectories();

            try (PrintStream out = new PrintStream(new FileOutputStreamPlus(schemaFile)))
            {
                SchemaCQLHelper.reCreateStatementsForSchemaCql(cfs.metadata(), cfs.keyspace.getMetadata())
                               .forEach(out::println);
            }
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, schemaFile);
        }
    }

    private void removeSnapshotDirectory(File snapshotDir)
    {
        if (snapshotDir.exists())
        {
            logger.trace("Removing snapshot directory {}", snapshotDir);
            try
            {
                FileUtils.deleteRecursiveWithThrottle(snapshotDir, snapshotRateLimiter);
            }
            catch (RuntimeException ex)
            {
                if (!snapshotDir.exists())
                    return; // ignore
                throw ex;
            }
        }
    }
}
