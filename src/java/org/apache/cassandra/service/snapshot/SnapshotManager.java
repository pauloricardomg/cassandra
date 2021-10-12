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


import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;

import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ExecutorUtils;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.db.Directories.getSnapshotDirectory;
import static org.apache.cassandra.utils.FBUtilities.now;

public class SnapshotManager {

    private static final ScheduledExecutorPlus executor = executorFactory().scheduled(false, "SnapshotCleanup");

    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

    private final long initialDelaySeconds;
    private final long cleanupPeriodSeconds;

    @VisibleForTesting
    protected volatile ScheduledFuture cleanupTaskFuture;

    /**
     * Map of "$ks:$table_name:$table_id:$tag" -> TableSnapshot
     */
    private final Map<String, TableSnapshot> liveSnapshots = new HashMap<>();

    /**
     * Expiring ssnapshots ordered by expiration date, to allow only iterating over snapshots
     * that need to be removed on {@link this#clearExpiredSnapshots()}
     */
    private final PriorityQueue<TableSnapshot> expiringSnapshots = new PriorityQueue<>(Comparator.comparing(x -> x.getExpiresAt()));

    public SnapshotManager()
    {
        this(CassandraRelevantProperties.SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS.getInt(),
             CassandraRelevantProperties.SNAPSHOT_CLEANUP_PERIOD_SECONDS.getInt());
    }

    @VisibleForTesting
    protected SnapshotManager(long initialDelaySeconds, long cleanupPeriodSeconds)
    {
        this.initialDelaySeconds = initialDelaySeconds;
        this.cleanupPeriodSeconds = cleanupPeriodSeconds;
    }

    public Collection<TableSnapshot> getExpiringSnapshots()
    {
        return expiringSnapshots;
    }

    public synchronized void start()
    {
        loadSnapshots();
        resumeSnapshotCleanup();
    }

    public synchronized void stop() throws InterruptedException, TimeoutException
    {
        expiringSnapshots.clear();
        if (cleanupTaskFuture != null)
        {
            cleanupTaskFuture.cancel(false);
            cleanupTaskFuture = null;
        }
    }

    @VisibleForTesting
    protected synchronized void loadSnapshots()
    {
        loadSnapshots(new SnapshotLoader(DatabaseDescriptor.getAllDataFileLocations()));
    }

    @VisibleForTesting
    protected synchronized void loadSnapshots(SnapshotLoader loader)
    {
        addSnapshots(loader.loadSnapshots());
    }

    @VisibleForTesting
    protected synchronized void addSnapshots(Collection<TableSnapshot> snapshots)
    {
        logger.debug("Adding snapshots: {}", snapshots.stream().map(s -> s.getId()).collect(Collectors.toList()));
        snapshots.forEach(this::addSnapshot);
    }

    public synchronized void addSnapshot(TableSnapshot snapshot)
    {
        if (liveSnapshots.containsKey(snapshot.getId()))
        {
            logger.warn("Overwriting existing snaspshot {}", snapshot.getId());
        }
        liveSnapshots.put(snapshot.getId(), snapshot);
        if (snapshot.isExpiring())
        {
            logger.trace("Adding expiring snapshot {}", snapshot);
            expiringSnapshots.add(snapshot);
        }
    }

    public Set<TableSnapshot> getSnapshots(String keyspaceName)
    {
        return getSnapshots(TableSnapshot.sameKeyspacePredicate(keyspaceName));
    }

    public Set<TableSnapshot> getSnapshots(UUID tableId)
    {
        return getSnapshots(TableSnapshot.sameTablePredicate(tableId));
    }

    public synchronized Set<TableSnapshot> getSnapshots(Predicate<TableSnapshot> filter)
    {
        return liveSnapshots.values().stream()
                                     .filter(filter)
                                     .collect(Collectors.toSet());
    }

    // TODO: Support pausing snapshot cleanup
    private synchronized void resumeSnapshotCleanup()
    {
        if (cleanupTaskFuture == null)
        {
            logger.info("Scheduling expired snapshot cleanup with initialDelaySeconds={} and cleanupPeriodSeconds={}", initialDelaySeconds, cleanupPeriodSeconds);
            cleanupTaskFuture = executor.scheduleWithFixedDelay(this::clearExpiredSnapshots, initialDelaySeconds,
                                                                cleanupPeriodSeconds, TimeUnit.SECONDS);
        }
    }

    @VisibleForTesting
    protected synchronized void clearExpiredSnapshots()
    {
        Instant now = now();
        while (!expiringSnapshots.isEmpty() && expiringSnapshots.peek().isExpired(now))
        {
            TableSnapshot expiredSnapshot = expiringSnapshots.peek();
            logger.debug("Removing expired snapshot {}.", expiredSnapshot);
            clearSnapshot(expiredSnapshot);
        }
    }

    /**
     * Deletes snapshot and remove it from manager
     */
    protected synchronized void clearSnapshot(TableSnapshot snapshot)
    {
        logger.debug("Clearing snapshot " + snapshot);
        for (File snapshotDir : snapshot.getDirectories())
        {
            removeSnapshotDirectory(DatabaseDescriptor.getSnapshotRateLimiter(), snapshotDir);
        }
        expiringSnapshots.remove(snapshot);
        liveSnapshots.remove(snapshot.getId());
    }

    @VisibleForTesting
    public static void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownNowAndWait(timeout, unit, executor);
    }

    public synchronized Collection<TableSnapshot> clearSnapshots(Predicate<TableSnapshot> predicate)
    {
        Collection<TableSnapshot> toClear = getSnapshots(predicate);
        toClear.forEach(this::clearSnapshot);
        return toClear;
    }

    public synchronized boolean exists(String tag)
    {
        return liveSnapshots.values().stream().anyMatch(t -> t.getTag().equals(tag));
    }

    public long trueSnapshotSize(Predicate<TableSnapshot> predicate)
    {
        Collection<TableSnapshot> matchingSnapshots = getSnapshots(predicate);
        return matchingSnapshots.stream().mapToLong(s -> s.computeTrueSizeBytes()).sum();
    }

    public static void clearSnapshot(String snapshotName, List<File> tableDirectories)
    {
        // If snapshotName is empty or null, we will delete the entire snapshot directory
        String tag = snapshotName == null ? StorageService.ALL_SNAPSHOTS_TAG : snapshotName;
        for (File tableDir : tableDirectories)
        {
            File snapshotDir = getSnapshotDirectory(tag, tableDir);
            if (snapshotDir.exists())
            {
                logger.debug("Removing snapshot directory {}", snapshotDir);
                FileUtils.deleteRecursiveWithThrottle(snapshotDir, DatabaseDescriptor.getSnapshotRateLimiter());
            }
        }
    }

    public static void removeSnapshotDirectory(RateLimiter snapshotRateLimiter, File snapshotDir)
    {
        if (snapshotDir.exists())
        {
            logger.trace("Removing snapshot directory {}", snapshotDir);
            try
            {
                FileUtils.deleteRecursiveWithThrottle(snapshotDir, snapshotRateLimiter);
            }
            catch (FSWriteError e)
            {
                throw e;
            }
        }
    }
}
