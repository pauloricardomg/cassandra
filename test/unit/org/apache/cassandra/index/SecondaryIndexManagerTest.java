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

package org.apache.cassandra.index;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableBeforeAddNotification;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.concurrent.Refs;

/**
 * Tests for {@link SecondaryIndexManager}.
 */
public class SecondaryIndexManagerTest extends CQLTester
{

    @Test
    public void notificationHandling() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        String indexName = createIndex("CREATE INDEX ON %s(c)");
        waitForIndex(KEYSPACE, tableName, indexName);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String builtIndexesQuery = String.format("SELECT * FROM %s.\"%s\"",
                                                 SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                                 SystemKeyspace.BUILT_INDEXES);
        assertRows(execute(builtIndexesQuery), row(KEYSPACE, indexName));

        try (Refs<SSTableReader> sstables = Refs.ref(cfs.getSSTables(SSTableSet.CANONICAL)))
        {
            // Test regular notifcations flow
            cfs.indexManager.handleNotification(new SSTableBeforeAddNotification(sstables, null), cfs.getTracker());
            assertEmpty(execute(builtIndexesQuery));
            cfs.indexManager.handleNotification(new SSTableAddedNotification(sstables, null), cfs.getTracker());
            assertRows(execute(builtIndexesQuery), row(KEYSPACE, indexName));

            // Test simultanous rebuildings
            cfs.indexManager.handleNotification(new SSTableBeforeAddNotification(sstables, null), cfs.getTracker());
            assertEmpty(execute(builtIndexesQuery));
            cfs.indexManager.handleNotification(new SSTableBeforeAddNotification(sstables, null), cfs.getTracker());
            assertEmpty(execute(builtIndexesQuery));
            cfs.indexManager.handleNotification(new SSTableAddedNotification(sstables, null), cfs.getTracker());
            assertEmpty(execute(builtIndexesQuery));
            cfs.indexManager.handleNotification(new SSTableAddedNotification(sstables, null), cfs.getTracker());
            assertRows(execute(builtIndexesQuery), row(KEYSPACE, indexName));

            // Test added notification without a preceeding before-add notification
            cfs.indexManager.handleNotification(new SSTableAddedNotification(sstables, null), cfs.getTracker());
            assertRows(execute(builtIndexesQuery), row(KEYSPACE, indexName));

            // Test regular flow again now that we have just received an out of order added notification
            cfs.indexManager.handleNotification(new SSTableBeforeAddNotification(sstables, null), cfs.getTracker());
            assertEmpty(execute(builtIndexesQuery));
            cfs.indexManager.handleNotification(new SSTableAddedNotification(sstables, null), cfs.getTracker());
            assertRows(execute(builtIndexesQuery), row(KEYSPACE, indexName));
        }
    }
}
