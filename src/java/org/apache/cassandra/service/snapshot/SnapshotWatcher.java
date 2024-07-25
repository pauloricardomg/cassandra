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
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.ExecutorUtils;

import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;

/**
 * SnapshotWatcher watches snapshot directories. When a directory is removed from disk manually,
 * it will clean respective snapshot from SnapshotManager. The result of doing so is that when somebody
 * removes a snapshot via other means from e.g. nodetool clearsnapshot, such snapshot will not be present
 * in SnapshotManager anymore hence nodetool listsnapshots nor system_views.snapshots will not display it either.
 */
public class SnapshotWatcher implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(SnapshotWatcher.class);

    private final Consumer<Path> removedSnapshotConsumer;
    private WatchService watchService;
    private ExecutorService executor;
    private final Map<WatchKey, Path> watchKeyPathMap = new ConcurrentHashMap<>();
    private boolean started = false;

    private Future<?> watcherFuture;

    public SnapshotWatcher(Consumer<Path> removedSnapshotConsumer)
    {
        this.removedSnapshotConsumer = removedSnapshotConsumer;
    }

    public void watch(TableSnapshot snapshot)
    {
        if (!started)
            return;

        Collection<File> directories = snapshot.getDirectories();
        if (directories == null)
            return;

        for (File snapshotDir : directories)
        {
            Path rootSnapshotsDir = snapshotDir.parent().toPath();
            if (!watchKeyPathMap.containsValue(rootSnapshotsDir))
            {
                try
                {
                    WatchKey watchKey = rootSnapshotsDir.register(watchService, ENTRY_DELETE);
                    logger.trace("Watching snapshots dir {}", rootSnapshotsDir);
                    watchKeyPathMap.put(watchKey, rootSnapshotsDir);
                }
                catch (IOException ex)
                {
                    logger.warn("Unable to register watch service for path " + rootSnapshotsDir, ex);
                }
            }
        }
    }

    @VisibleForTesting
    Collection<Path> getWatchedDirs()
    {
        return watchKeyPathMap.values();
    }

    public void unwatch(Path snapshotsRootDir)
    {
        if (!started)
            return;

        logger.trace("Unwatching snapshots dir {}", snapshotsRootDir);

        List<WatchKey> watchKeysToRemove = new ArrayList<>();
        for (Map.Entry<WatchKey, Path> entry : watchKeyPathMap.entrySet())
        {
            if (entry.getValue().equals(snapshotsRootDir))
            {
                WatchKey watchKey = entry.getKey();
                watchKey.cancel();
                watchKeysToRemove.add(watchKey);
            }
        }

        for (WatchKey watchKeyToRemove : watchKeysToRemove)
            watchKeyPathMap.remove(watchKeyToRemove);
    }

    public boolean isEnabled()
    {
        return DatabaseDescriptor.isSnapshotWatcherEnabled();
    }

    public boolean isStarted()
    {
        return started;
    }

    public synchronized void start()
    {
        if (!isEnabled())
            return;

        if (started)
            return;

        try
        {
            if (watchService == null)
                watchService = FileSystems.getDefault().newWatchService();
        }
        catch (IOException ex)
        {
            logger.warn("Unable to create a watch service on this filesystem!");
            return;
        }

        executor = getExecutorService();

        watcherFuture = executor.submit(() -> {
            while (!Thread.currentThread().isInterrupted())
            {
                WatchKey key;

                try
                {
                    while ((key = watchService.take()) != null)
                    {
                        Path rootSnapshotDir = watchKeyPathMap.get(key);

                        for (WatchEvent<?> event : key.pollEvents())
                            removedSnapshotConsumer.accept(rootSnapshotDir.resolve(event.context().toString()));

                        key.reset();
                    }
                }
                catch (ClosedWatchServiceException | InterruptedException ex)
                {
                    logger.debug("Watcher was closed");
                    Thread.currentThread().interrupt();
                }
            }
        });


        started = true;
    }

    @Override
    public synchronized void close()
    {
        if (!started)
            return;

        for (Map.Entry<WatchKey, Path> entry : watchKeyPathMap.entrySet())
            entry.getKey().cancel();

        watchKeyPathMap.clear();

        try
        {
            if (watchService != null)
                watchService.close();
        }
        catch (Exception ex)
        {
            logger.error("Error occured while closing snapshot watcher: {}", ex.getMessage());
        }

        try
        {
            if (watcherFuture != null)
                watcherFuture.cancel(true);
            if (executor != null)
                ExecutorUtils.shutdownNowAndWait(1, TimeUnit.MINUTES, executor);
        }
        catch (Exception ex)
        {
            logger.error("Error occured while shutting down SnapshotWatcher executor: {}", ex.getMessage());
        }

        watcherFuture = null;
        watchService = null;
        executor = null;
        started = false;
    }

    private static ExecutorService getExecutorService()
    {
        return ExecutorFactory.Global.executorFactory().localAware().sequential("SnapshotWatcher");
    }
}
