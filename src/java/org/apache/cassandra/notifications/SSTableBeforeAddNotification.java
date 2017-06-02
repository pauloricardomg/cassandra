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
package org.apache.cassandra.notifications;

import java.util.Optional;
import javax.annotation.Nullable;

import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * Notification sent before SSTables are added to their {@link org.apache.cassandra.db.ColumnFamilyStore}.
 * Note that this notification doesn't mean that the tables have been added, just that they are about to be added.
 * The effective addition, if it's successful, will be notified by {@link SSTableAddedNotification}.
 */
public class SSTableBeforeAddNotification implements INotification
{
    /** The SSTables that are going to be added */
    public final Iterable<SSTableReader> adding;

    /** The memtable from which the tables come when they are being added due to a flush, {@code null} otherwise. */
    @Nullable
    private final Memtable memtable;

    /**
     * Creates a new {@code SSTableBeforeAddNotification} for the specified SSTables and optional memtable.
     *
     * @param adding   the SSTables that are going to be added
     * @param memtable the memtable from which the tables come when they are going to be added due to a memtable flush,
     *                 or {@code null} if they don't come from a flush
     */
    public SSTableBeforeAddNotification(Iterable<SSTableReader> adding, @Nullable Memtable memtable)
    {
        this.adding = adding;
        this.memtable = memtable;
    }

    /**
     * Returns the memtable from which the tables come when they are going to be added due to a memtable flush. If not,
     * an empty Optional should be returned.
     *
     * @return the origin memtable in case of a flush, {@link Optional#empty()} otherwise
     */
    public Optional<Memtable> memtable()
    {
        return Optional.ofNullable(memtable);
    }
}
