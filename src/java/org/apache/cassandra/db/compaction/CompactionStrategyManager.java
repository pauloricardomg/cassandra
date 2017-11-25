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


import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.index.Index;
import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.notifications.*;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.ActiveRepairService;

/**
 * Manages the compaction strategies
 *
 * Currently has two instances of actual compaction strategies er data directory - one for repaired data and one for
 * unrepaired data. This is done to be able to totally separate the different sets of sstables.
 */

public class CompactionStrategyManager implements INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionStrategyManager.class);
    public final CompactionLogger compactionLogger;
    private final ColumnFamilyStore cfs;

    private final Supplier<DiskBoundaries> boundariesSupplier;
    private final boolean partitionSSTablesByTokenRange;
    private volatile boolean enabled = true;
    private volatile boolean isActive = true;
    private volatile CompactionParams params;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    private final AtomicReference<CompactionStrategies> strategies = new AtomicReference<>();

    private Directories.DataDirectory[] locations;

    public CompactionStrategyManager(ColumnFamilyStore cfs)
    {
        this(cfs, cfs::getDiskBoundaries, cfs.getPartitioner().splitter().isPresent());
    }

    @VisibleForTesting
    public CompactionStrategyManager(ColumnFamilyStore cfs, Supplier<DiskBoundaries> boundariesSupplier,
                                     boolean partitionSSTablesByTokenRange)
    {
        cfs.getTracker().subscribe(this);
        logger.trace("{} subscribed to the data tracker.", this);
        this.cfs = cfs;
        this.compactionLogger = new CompactionLogger(cfs, this);
        this.boundariesSupplier = boundariesSupplier;
        this.partitionSSTablesByTokenRange = partitionSSTablesByTokenRange;
        reload(cfs.metadata);
        params = cfs.metadata.params.compaction;
        locations = getDirectories().getWriteableLocations();
        enabled = cfs.metadata.params.compaction.isEnabled();
    }

    /**
     * Return the next background task
     *
     * Returns a task for the compaction strategy that needs it the most (most estimated remaining tasks)
     *
     */
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        readLock.lock();
        try
        {
            if (!isEnabled())
                return null;
            return compactionStrategies().getNextBackgroundTask(gcBefore);
        }
        finally
        {
            readLock.unlock();
        }
    }

    public boolean isEnabled()
    {
        return enabled && isActive;
    }

    public boolean isActive()
    {
        return isActive;
    }

    public void resume()
    {
        writeLock.lock();
        try
        {
            isActive = true;
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * pause compaction while we cancel all ongoing compactions
     *
     * Separate call from enable/disable to not have to save the enabled-state externally
      */
    public void pause()
    {
        writeLock.lock();
        try
        {
            isActive = false;
        }
        finally
        {
            writeLock.unlock();
        }

    }

    /**
     * return the compaction strategy for the given sstable
     *
     * returns differently based on the repaired status and which vnode the compaction strategy belongs to
     * @param sstable
     * @return
     */
    protected AbstractCompactionStrategy getCompactionStrategyFor(SSTableReader sstable)
    {
        return compactionStrategies().getCompactionStrategyFor(sstable);
    }


    public void shutdown()
    {
        writeLock.lock();
        try
        {
            isActive = false;
            compactionStrategies().shutdown();
            compactionLogger.disable();
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * @deprecated use {@link this#maybeReload()} instead
     */
    @Deprecated
    public boolean maybeReload(CFMetaData metadata)
    {
        CompactionStrategies currentStrategy = strategies.get();
        if (currentStrategy.shouldReload())
            return false;

        synchronized (this)
        {
            currentStrategy = strategies.get();
            if (!currentStrategy.shouldReload())
                return false;
            if (currentStrategy.hasParametersChanged())
                logger.debug("Recreating compaction strategy - compaction parameters changed for {}.{}", cfs.keyspace.getName(), cfs.getTableName());
            else if (currentStrategy.hasBoundariesChanged())
                logger.debug("Recreating compaction strategy - disk boundaries are out of date for {}.{}.", cfs.keyspace.getName(), cfs.getTableName());
            reload(metadata);
            return true;
        }
    }

    public boolean maybeReload()
    {
        return maybeReload(cfs.metadata);
    }

    /**
     * Reload the compaction strategies
     *
     * Called after changing configuration and at startup.
     * @param metadata
     */
    private void reload(CFMetaData metadata)
    {
        boolean disabledWithJMX = !enabled && shouldBeEnabled();

        setStrategy(metadata.params.compaction);

        if (disabledWithJMX || !shouldBeEnabled())
            disable();
        else
            enable();
        strategies.get().init();
    }

    public void setNewLocalCompactionStrategy(CompactionParams params)
    {
        logger.info("Switching local compaction strategy from {} to {}}", this.params, params);
        synchronized (this)
        {
            setStrategy(params);
            if (shouldBeEnabled())
                enable();
            else
                disable();
            strategies.get().init();;
        }
    }

    public int getUnleveledSSTables()
    {
        return compactionStrategies().getUnleveledSSTables();
    }

    public int getLevelFanoutSize()
    {
        return compactionStrategies().fanout;
    }

    public int[] getSSTableCountPerLevel()
    {
        return compactionStrategies().getSSTableCountPerLevel();
    }

    public boolean shouldDefragment()
    {
        return compactionStrategies().shouldDefragment;
    }

    public Directories getDirectories()
    {
        return compactionStrategies().getDirectories();
    }

    private void handleFlushNotification(Iterable<SSTableReader> added)
    {
        compactionStrategies().addSSTables(added);
    }

    private void handleListChangedNotification(Iterable<SSTableReader> added, Iterable<SSTableReader> removed)
    {
        compactionStrategies().replaceSSTables(added, removed);
    }

    private void handleRepairStatusChangedNotification(Iterable<SSTableReader> sstables)
    {
        compactionStrategies().updateRepairStatus(sstables);
    }

    private void handleDeletingNotification(SSTableReader deleted)
    {
        writeLock.lock();
        try
        {
            getCompactionStrategyFor(deleted).removeSSTable(deleted);
        }
        finally
        {
            writeLock.unlock();
        }
    }

    public void handleNotification(INotification notification, Object sender)
    {
        // If reloaded, SSTables will be placed in their correct locations
        // so there is no need to process notification
        if (maybeReload())
            return;

        if (notification instanceof SSTableAddedNotification)
        {
            handleFlushNotification(((SSTableAddedNotification) notification).added);
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            SSTableListChangedNotification listChangedNotification = (SSTableListChangedNotification) notification;
            handleListChangedNotification(listChangedNotification.added, listChangedNotification.removed);
        }
        else if (notification instanceof SSTableRepairStatusChanged)
        {
            handleRepairStatusChangedNotification(((SSTableRepairStatusChanged) notification).sstables);
        }
        else if (notification instanceof SSTableDeletingNotification)
        {
            handleDeletingNotification(((SSTableDeletingNotification) notification).deleting);
        }
    }

    public void enable()
    {
        writeLock.lock();
        try
        {
            compactionStrategies().enable();
            // enable this last to make sure the compactionStrategies().are ready to get calls.
            enabled = true;
        }
        finally
        {
            writeLock.unlock();
        }
    }

    public void disable()
    {
        writeLock.lock();
        try
        {
            // disable this first avoid asking disabled compactionStrategies().for compaction tasks
            enabled = false;
            compactionStrategies().disable();
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * Create ISSTableScanners from the given sstables
     *
     * Delegates the call to the compaction compactionStrategies().to allow LCS to create a scanner
     * @param sstables
     * @param ranges
     * @return
     */
    @SuppressWarnings("resource")
    public AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables,  Collection<Range<Token>> ranges)
    {
        return compactionStrategies().getScanners(sstables, ranges);
    }

    public AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables)
    {
        return getScanners(sstables, null);
    }

    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup)
    {
        return compactionStrategies().groupSSTablesForAntiCompaction(sstablesToGroup);
    }

    public long getMaxSSTableBytes()
    {
        return compactionStrategies().getMaxSSTableBytes();
    }

    public AbstractCompactionTask getCompactionTask(LifecycleTransaction txn, int gcBefore, long maxSSTableBytes)
    {
        validateForCompaction(txn.originals());
        return getCompactionStrategyFor(txn.originals().iterator().next()).getCompactionTask(txn, gcBefore, maxSSTableBytes);
    }

    private void validateForCompaction(Iterable<SSTableReader> input)
    {
        compactionStrategies().validateForCompaction(input);
    }

    public Collection<AbstractCompactionTask> getMaximalTasks(final int gcBefore, final boolean splitOutput)
    {
        // runWithCompactionsDisabled cancels active compactions and disables them, then we are able
        // to make the repaired/unrepaired compactionStrategies().mark their own sstables as compacting. Once the
        // sstables are marked the compactions are re-enabled
        return cfs.runWithCompactionsDisabled(new Callable<Collection<AbstractCompactionTask>>()
        {
            @Override
            public Collection<AbstractCompactionTask> call()
            {
                return compactionStrategies().getMaximalTasks(gcBefore, splitOutput);
            }
        }, false, false);
    }

    /**
     * Return a list of compaction tasks corresponding to the sstables requested. Split the sstables according
     * to whether they are repaired or not, and by disk location. Return a task per disk location and repair status
     * group.
     *
     * @param sstables the sstables to compact
     * @param gcBefore gc grace period, throw away tombstones older than this
     * @return a list of compaction tasks corresponding to the sstables requested
     */
    public List<AbstractCompactionTask> getUserDefinedTasks(Collection<SSTableReader> sstables, int gcBefore)
    {
        return compactionStrategies().getUserDefinedTasks(sstables, gcBefore);
    }

    /**
     * @deprecated use {@link #getUserDefinedTasks(Collection, int)} instead.
     */
    @Deprecated()
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        validateForCompaction(sstables);
        List<AbstractCompactionTask> tasks = getUserDefinedTasks(sstables, gcBefore);
        assert tasks.size() == 1;
        return tasks.get(0);
    }

    public int getEstimatedRemainingTasks()
    {
        return compactionStrategies().getEstimatedRemainingTasks();
    }

    public boolean shouldBeEnabled()
    {
        return params.isEnabled();
    }

    public String getName()
    {
        return compactionStrategies().getName();
    }

    public List<List<AbstractCompactionStrategy>> getStrategies()
    {
        return compactionStrategies().getStrategies();
    }

    private void setStrategy(CompactionParams params)
    {
        CompactionStrategies previous = strategies.get();
        if (previous != null)
            previous.shutdown();
        strategies.set(new CompactionStrategies(params));
    }

    public CompactionParams getCompactionParams()
    {
        return params;
    }

    public boolean onlyPurgeRepairedTombstones()
    {
        return Boolean.parseBoolean(params.options().get(AbstractCompactionStrategy.ONLY_PURGE_REPAIRED_TOMBSTONES));
    }

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor,
                                                       long keyCount,
                                                       long repairedAt,
                                                       MetadataCollector collector,
                                                       SerializationHeader header,
                                                       Collection<Index> indexes,
                                                       LifecycleTransaction txn)
    {
        return compactionStrategies().createSSTableMultiWriter(descriptor, keyCount, repairedAt, collector, header, indexes, txn);
    }

    public boolean isRepaired(AbstractCompactionStrategy strategy)
    {
        return compactionStrategies().isRepaired(strategy);
    }

    public List<String> getStrategyFolders(AbstractCompactionStrategy strategy)
    {
        return compactionStrategies().getStrategyFolders(strategy);
    }

    public boolean supportsEarlyOpen()
    {
        return compactionStrategies().supportsEarlyOpen();
    }

    class CompactionStrategies
    {
        private final List<AbstractCompactionStrategy> repaired = new ArrayList<>();
        private final List<AbstractCompactionStrategy> unrepaired = new ArrayList<>();
        private final DiskBoundaries currentBoundaries;
        private final boolean shouldDefragment;
        private final int fanout;

        /*
            We keep a copy of the schema compaction parameters here to be able to decide if we
            should update the compaction strategy in maybeReloadCompactionStrategy() due to an ALTER.

            If a user changes the local compaction strategy and then later ALTERs a compaction parameter,
            we will use the new compaction parameters.
         */
        private final CompactionParams schemaCompactionParams;

        CompactionStrategies(CompactionParams params)
        {
            this.currentBoundaries = boundariesSupplier.get();
            this.schemaCompactionParams = cfs.metadata.params.compaction;
            if (partitionSSTablesByTokenRange)
            {
                locations = cfs.getDirectories().getWriteableLocations();
                for (int i = 0; i < locations.length; i++)
                {
                    repaired.add(CFMetaData.createCompactionStrategyInstance(cfs, params));
                    unrepaired.add(CFMetaData.createCompactionStrategyInstance(cfs, params));
                }
            }
            else
            {
                repaired.add(CFMetaData.createCompactionStrategyInstance(cfs, params));
                unrepaired.add(CFMetaData.createCompactionStrategyInstance(cfs, params));
            }
            shouldDefragment = repaired.get(0).shouldDefragment();
            fanout = (repaired.get(0) instanceof LeveledCompactionStrategy) ? ((LeveledCompactionStrategy) repaired.get(0)).getLevelFanoutSize() : LeveledCompactionStrategy.DEFAULT_LEVEL_FANOUT_SIZE;
        }

        public void init()
        {
            for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
            {
                if (sstable.openReason != SSTableReader.OpenReason.EARLY)
                    getCompactionStrategyFor(sstable).addSSTable(sstable);
            }
            repaired.forEach(AbstractCompactionStrategy::startup);
            unrepaired.forEach(AbstractCompactionStrategy::startup);

            repaired.forEach(AbstractCompactionStrategy::startup);
            unrepaired.forEach(AbstractCompactionStrategy::startup);
            if (Stream.concat(repaired.stream(), unrepaired.stream()).anyMatch(cs -> cs.logAll))
                compactionLogger.enable();
        }

        public AbstractCompactionStrategy getCompactionStrategyFor(SSTableReader sstable)
        {
            int index = getCompactionStrategyIndex(sstable);
            if (sstable.isRepaired())
                return repaired.get(index);
            else
                return unrepaired.get(index);
        }

        public void shutdown()
        {
            repaired.forEach(AbstractCompactionStrategy::shutdown);
            unrepaired.forEach(AbstractCompactionStrategy::shutdown);
        }

        private boolean shouldReload()
        {
            return hasParametersChanged() || hasBoundariesChanged(); //disk boundaries changed?
        }

        private boolean hasBoundariesChanged()
        {
            return currentBoundaries.isOutOfDate();
        }

        private boolean hasParametersChanged()
        {
            return !cfs.metadata.params.compaction.equals(schemaCompactionParams);
        }

        public String getName()
        {
            return unrepaired.get(0).getName();
        }

        public <T extends AbstractCompactionStrategy> Class<T> getStrategyClass()
        {
            return (Class<T>) unrepaired.get(0).getClass();
        }

        public int getUnleveledSSTables()
        {
            if (repaired.get(0) instanceof LeveledCompactionStrategy && unrepaired.get(0) instanceof LeveledCompactionStrategy)
            {
                int count = 0;
                for (AbstractCompactionStrategy strategy : repaired)
                    count += ((LeveledCompactionStrategy) strategy).getLevelSize(0);
                for (AbstractCompactionStrategy strategy : unrepaired)
                    count += ((LeveledCompactionStrategy) strategy).getLevelSize(0);
                return count;
            }
            return 0;
        }

        public int[] getSSTableCountPerLevel()
        {
            if (repaired.get(0) instanceof LeveledCompactionStrategy && unrepaired.get(0) instanceof LeveledCompactionStrategy)
            {
                int[] res = new int[LeveledManifest.MAX_LEVEL_COUNT];
                for (AbstractCompactionStrategy strategy : repaired)
                {
                    int[] repairedCountPerLevel = ((LeveledCompactionStrategy) strategy).getAllLevelSize();
                    res = sumArrays(res, repairedCountPerLevel);
                }
                for (AbstractCompactionStrategy strategy : unrepaired)
                {
                    int[] unrepairedCountPerLevel = ((LeveledCompactionStrategy) strategy).getAllLevelSize();
                    res = sumArrays(res, unrepairedCountPerLevel);
                }
                return res;
            }
            return null;
        }

        public Directories getDirectories()
        {
            assert repaired.get(0).getClass().equals(unrepaired.get(0).getClass());
            return repaired.get(0).getDirectories();
        }

        public void replaceSSTables(Iterable<SSTableReader> added, Iterable<SSTableReader> removed)
        {
            int locationSize = partitionSSTablesByTokenRange ? currentBoundaries.directories.size() : 1;

            List<Set<SSTableReader>> repairedRemoved = new ArrayList<>(locationSize);
            List<Set<SSTableReader>> repairedAdded = new ArrayList<>(locationSize);
            List<Set<SSTableReader>> unrepairedRemoved = new ArrayList<>(locationSize);
            List<Set<SSTableReader>> unrepairedAdded = new ArrayList<>(locationSize);

            for (int i = 0; i < locationSize; i++)
            {
                repairedRemoved.add(new HashSet<>());
                repairedAdded.add(new HashSet<>());
                unrepairedRemoved.add(new HashSet<>());
                unrepairedAdded.add(new HashSet<>());
            }

            for (SSTableReader sstable : removed)
            {
                int i = getCompactionStrategyIndex(sstable);
                if (sstable.isRepaired())
                    repairedRemoved.get(i).add(sstable);
                else
                    unrepairedRemoved.get(i).add(sstable);
            }
            for (SSTableReader sstable : added)
            {
                int i = getCompactionStrategyIndex(sstable);
                if (sstable.isRepaired())
                    repairedAdded.get(i).add(sstable);
                else
                    unrepairedAdded.get(i).add(sstable);
            }
            for (int i = 0; i < locationSize; i++)
            {
                if (!repairedRemoved.get(i).isEmpty())
                    repaired.get(i).replaceSSTables(repairedRemoved.get(i), repairedAdded.get(i));
                else
                    repaired.get(i).addSSTables(repairedAdded.get(i));

                if (!unrepairedRemoved.get(i).isEmpty())
                    unrepaired.get(i).replaceSSTables(unrepairedRemoved.get(i), unrepairedAdded.get(i));
                else
                    unrepaired.get(i).addSSTables(unrepairedAdded.get(i));
            }
        }

        public void addSSTables(Iterable<SSTableReader> added)
        {
            compactionStrategies().replaceSSTables(added, Collections.emptyList());
        }

        public void updateRepairStatus(Iterable<SSTableReader> sstables)
        {
            for (SSTableReader sstable : sstables)
            {
                int index = getCompactionStrategyIndex(sstable);
                if (sstable.isRepaired())
                {
                    unrepaired.get(index).removeSSTable(sstable);
                    repaired.get(index).addSSTable(sstable);
                }
                else
                {
                    repaired.get(index).removeSSTable(sstable);
                    unrepaired.get(index).addSSTable(sstable);
                }
            }
        }

        public void enable()
        {
            if (repaired != null)
                repaired.forEach(AbstractCompactionStrategy::enable);
            if (unrepaired != null)
                unrepaired.forEach(AbstractCompactionStrategy::enable);
        }

        public void disable()
        {
            if (repaired != null)
                repaired.forEach(AbstractCompactionStrategy::disable);
            if (unrepaired != null)
                unrepaired.forEach(AbstractCompactionStrategy::disable);
        }

        public AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges)
        {
            assert repaired.size() == unrepaired.size();
            List<Set<SSTableReader>> repairedSSTables = new ArrayList<>();
            List<Set<SSTableReader>> unrepairedSSTables = new ArrayList<>();

            for (int i = 0; i < repaired.size(); i++)
            {
                repairedSSTables.add(new HashSet<>());
                unrepairedSSTables.add(new HashSet<>());
            }

            for (SSTableReader sstable : sstables)
            {
                if (sstable.isRepaired())
                    repairedSSTables.get(getCompactionStrategyIndex(sstable)).add(sstable);
                else
                    unrepairedSSTables.get(getCompactionStrategyIndex(sstable)).add(sstable);
            }

            List<ISSTableScanner> scanners = new ArrayList<>(sstables.size());
            for (int i = 0; i < repairedSSTables.size(); i++)
            {
                if (!repairedSSTables.get(i).isEmpty())
                    scanners.addAll(repaired.get(i).getScanners(repairedSSTables.get(i), ranges).scanners);
            }
            for (int i = 0; i < unrepairedSSTables.size(); i++)
            {
                if (!unrepairedSSTables.get(i).isEmpty())
                    scanners.addAll(unrepaired.get(i).getScanners(unrepairedSSTables.get(i), ranges).scanners);
            }

            return new AbstractCompactionStrategy.ScannerList(scanners);
        }

        /**
         * Get the correct compaction strategy for the given sstable. If the first token starts within a disk boundary, we
         * will add it to that compaction strategy.
         *
         * In the case we are upgrading, the first compaction strategy will get most files - we do not care about which disk
         * the sstable is on currently (unless we don't know the local tokens yet). Once we start compacting we will write out
         * sstables in the correct locations and give them to the correct compaction strategy instance.
         *
         * @param sstable
         * @return
         */
        private int getCompactionStrategyIndex(SSTableReader sstable)
        {
            //We only have a single compaction strategy when sstables are not
            //partitioned by token range
            if (!partitionSSTablesByTokenRange)
                return 0;

            return currentBoundaries.getDiskIndex(sstable);
        }

        public int getEstimatedRemainingTasks()
        {
            int tasks = 0;
            for (AbstractCompactionStrategy strategy : repaired)
                tasks += strategy.getEstimatedRemainingTasks();
            for (AbstractCompactionStrategy strategy : unrepaired)
                tasks += strategy.getEstimatedRemainingTasks();
            return tasks;
        }

        public List<List<AbstractCompactionStrategy>> getStrategies()
        {
            return ImmutableList.copyOf(Arrays.asList(repaired, unrepaired));
        }

        public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup)
        {
            Map<Integer, List<SSTableReader>> groups = sstablesToGroup.stream().collect(Collectors.groupingBy((s) -> getCompactionStrategyIndex(s)));
            Collection<Collection<SSTableReader>> anticompactionGroups = new ArrayList<>();

            for (Map.Entry<Integer, List<SSTableReader>> group : groups.entrySet())
                anticompactionGroups.addAll(unrepaired.get(group.getKey()).groupSSTablesForAntiCompaction(group.getValue()));
            return anticompactionGroups;
        }

        public long getMaxSSTableBytes()
        {
            return unrepaired.get(0).getMaxSSTableBytes();
        }

        public void validateForCompaction(Iterable<SSTableReader> input)
        {
            SSTableReader firstSSTable = Iterables.getFirst(input, null);
            assert firstSSTable != null;
            boolean repaired = firstSSTable.isRepaired();
            int firstIndex = getCompactionStrategyIndex(firstSSTable);
            for (SSTableReader sstable : input)
            {
                if (sstable.isRepaired() != repaired)
                    throw new UnsupportedOperationException("You can't mix repaired and unrepaired data in a compaction");
                if (firstIndex != getCompactionStrategyIndex(sstable))
                    throw new UnsupportedOperationException("You can't mix sstables from different directories in a compaction");
            }
        }

        public Collection<AbstractCompactionTask> getMaximalTasks(int gcBefore, boolean splitOutput)
        {
            List<AbstractCompactionTask> tasks = new ArrayList<>();
            for (AbstractCompactionStrategy strategy : repaired)
            {
                Collection<AbstractCompactionTask> task = strategy.getMaximalTask(gcBefore, splitOutput);
                if (task != null)
                    tasks.addAll(task);
            }
            for (AbstractCompactionStrategy strategy : unrepaired)
            {
                Collection<AbstractCompactionTask> task = strategy.getMaximalTask(gcBefore, splitOutput);
                if (task != null)
                    tasks.addAll(task);
            }
            if (tasks.isEmpty())
                return null;
            return tasks;
        }

        public List<AbstractCompactionTask> getUserDefinedTasks(Collection<SSTableReader> sstables, int gcBefore)
        {
            List<AbstractCompactionTask> ret = new ArrayList<>();
            Map<Integer, List<SSTableReader>> repairedSSTables = sstables.stream()
                                                                         .filter(s -> !s.isMarkedSuspect() && s.isRepaired())
                                                                         .collect(Collectors.groupingBy((s) -> getCompactionStrategyIndex(s)));

            Map<Integer, List<SSTableReader>> unrepairedSSTables = sstables.stream()
                                                                           .filter(s -> !s.isMarkedSuspect() && !s.isRepaired())
                                                                           .collect(Collectors.groupingBy((s) -> getCompactionStrategyIndex(s)));


            for (Map.Entry<Integer, List<SSTableReader>> group : repairedSSTables.entrySet())
                ret.add(repaired.get(group.getKey()).getUserDefinedTask(group.getValue(), gcBefore));

            for (Map.Entry<Integer, List<SSTableReader>> group : unrepairedSSTables.entrySet())
                ret.add(unrepaired.get(group.getKey()).getUserDefinedTask(group.getValue(), gcBefore));

            return ret;
        }

        public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, MetadataCollector collector,
                                                           SerializationHeader header, Collection<Index> indexes, LifecycleTransaction txn)
        {
            if (repairedAt == ActiveRepairService.UNREPAIRED_SSTABLE)
            {
                return unrepaired.get(0).createSSTableMultiWriter(descriptor, keyCount, repairedAt, collector, header, indexes, txn);
            }
            else
            {
                return repaired.get(0).createSSTableMultiWriter(descriptor, keyCount, repairedAt, collector, header, indexes, txn);
            }
        }

        public boolean isRepaired(AbstractCompactionStrategy strategy)
        {
            return repaired.contains(strategy);
        }

        public List<String> getStrategyFolders(AbstractCompactionStrategy strategy)
        {
            List<Directories.DataDirectory> locations = currentBoundaries.directories;
            if (partitionSSTablesByTokenRange)
            {
                int unrepairedIndex = unrepaired.indexOf(strategy);
                if (unrepairedIndex > 0)
                {
                    return Collections.singletonList(locations.get(unrepairedIndex).location.getAbsolutePath());
                }
                int repairedIndex = repaired.indexOf(strategy);
                if (repairedIndex > 0)
                {
                    return Collections.singletonList(locations.get(repairedIndex).location.getAbsolutePath());
                }
            }
            List<String> folders = new ArrayList<>(locations.size());
            for (Directories.DataDirectory location : locations)
            {
                folders.add(location.location.getAbsolutePath());
            }
            return folders;
        }

        public boolean supportsEarlyOpen()
        {
            return repaired.get(0).supportsEarlyOpen();
        }

        public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
        {
            List<AbstractCompactionStrategy> strategies = new ArrayList<>();
            strategies.addAll(repaired);
            strategies .addAll(unrepaired);
            Collections.sort(strategies, (o1, o2) -> Ints.compare(o2.getEstimatedRemainingTasks(), o1.getEstimatedRemainingTasks()));

            for (AbstractCompactionStrategy strategy : strategies)
            {
                if (strategy.isActive)
                {
                    AbstractCompactionTask task = strategy.getNextBackgroundTask(gcBefore);
                    if (task != null)
                        return task;
                }
            }

            return null;
        }
    }

    private static int[] sumArrays(int[] a, int[] b)
    {
        int[] res = new int[Math.max(a.length, b.length)];
        for (int i = 0; i < res.length; i++)
        {
            if (i < a.length && i < b.length)
                res[i] = a[i] + b[i];
            else if (i < a.length)
                res[i] = a[i];
            else
                res[i] = b[i];
        }
        return res;
    }

    public CompactionStrategies compactionStrategies()
    {
        maybeReload();
        return strategies.get();
    }
}
