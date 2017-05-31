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

import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * Notification sent when SSTables are loaded from a source external to the node, such as streaming or sstableoader.
 * Note that this notification doesn't mean that the tables have been added to their {@link org.apache.cassandra.db.ColumnFamilyStore},
 * for this we have {@link SSTableAddedNotification}.
 */
public class SSTableLoadedNotification implements INotification
{
    public final Iterable<SSTableReader> added;

    public SSTableLoadedNotification(Iterable<SSTableReader> added)
    {
        this.added = added;
    }
}
