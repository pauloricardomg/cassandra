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

package org.apache.cassandra.streaming;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableUtils;
import org.apache.cassandra.io.sstable.SSTableWriter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class StreamLockFileTest extends SchemaLoader
{

    @Test
    public void testCleanupExistsAndDelete() throws Exception
    {
        // write temporary SSTables, but don't register them
        Set<String> content = new HashSet<>();
        content.add("test");
        content.add("test2");
        content.add("test3");
        SSTableWriter writer1 = SSTableUtils.prepare().writeWithoutFinishing(content);
        String keyspaceName = writer1.getKeyspaceName();
        String cfname = writer1.getColumnFamilyName();

        content = new HashSet<>();
        content.add("transfer1");
        content.add("transfer2");
        content.add("transfer3");
        SSTableWriter writer2 = SSTableUtils.prepare().writeWithoutFinishing(content);

        content = new HashSet<>();
        content.add("other1");
        content.add("other2");
        content.add("other3");
        SSTableWriter writer3 = SSTableUtils.prepare().writeWithoutFinishing(content);

        ColumnFamilyStore cfs = Keyspace.open(keyspaceName).getColumnFamilyStore(cfname);

        File lockfiledir = cfs.directories.getWriteableLocationAsFile(3 * 256L);
        StreamLockfile lockfile = new StreamLockfile(lockfiledir, UUID.randomUUID());

        lockfile.append(writer1);
        lockfile.append(writer2);
        lockfile.append(writer3);

        SSTableReader sstable1 = writer1.closeAndOpenReader();
        SSTableReader sstable2 = writer2.closeAndOpenReader();
        SSTableReader sstable3 = writer3.closeAndOpenReader();

        Set<Component> components = SSTable.componentsFor(sstable1.descriptor);

        assertAllComponentsExist(components, sstable1);
        assertAllComponentsExist(components, sstable2);
        assertAllComponentsExist(components, sstable3);

        lockfile.cleanup();

        assertAllComponentsDoNotExist(components, sstable1);
        assertAllComponentsDoNotExist(components, sstable2);
        assertAllComponentsDoNotExist(components, sstable3);

        assertTrue(lockfile.exists());

        lockfile.delete();

        assertFalse(lockfile.exists());
    }

    @Test
    public void testSkipOnCleanup() throws Exception
    {
        // write temporary SSTables, but don't register them
        Set<String> content = new HashSet<>();
        content.add("test");
        content.add("test2");
        content.add("test3");
        SSTableWriter writer1 = SSTableUtils.prepare().writeWithoutFinishing(content);
        String keyspaceName = writer1.getKeyspaceName();
        String cfname = writer1.getColumnFamilyName();

        content = new HashSet<>();
        content.add("transfer1");
        content.add("transfer2");
        content.add("transfer3");
        SSTableWriter writer2 = SSTableUtils.prepare().writeWithoutFinishing(content);

        content = new HashSet<>();
        content.add("other1");
        content.add("other2");
        content.add("other3");
        SSTableWriter writer3 = SSTableUtils.prepare().writeWithoutFinishing(content);

        ColumnFamilyStore cfs = Keyspace.open(keyspaceName).getColumnFamilyStore(cfname);

        File lockfiledir = cfs.directories.getWriteableLocationAsFile(3 * 256L);
        StreamLockfile lockfile = new StreamLockfile(lockfiledir, UUID.randomUUID());

        lockfile.append(writer1);
        lockfile.append(writer2);
        lockfile.append(writer3);

        SSTableReader sstable1 = writer1.closeAndOpenReader();
        SSTableReader sstable2 = writer2.closeAndOpenReader();
        SSTableReader sstable3 = writer3.closeAndOpenReader();

        Set<Component> components = SSTable.componentsFor(sstable1.descriptor);

        lockfile.skipOnCleanup(sstable1);
        lockfile.skipOnCleanup(sstable2);

        assertAllComponentsExist(components, sstable1);
        assertAllComponentsExist(components, sstable2);
        assertAllComponentsExist(components, sstable3);

        //cleanup returns false, since some sstables were skipped
        lockfile.cleanup();

        //sstables 1 and 2 were skipped, so they're still present
        assertAllComponentsExist(components, sstable1);
        assertAllComponentsExist(components, sstable2);

        //sstable3 was the only cleaned up, since it was not skipped
        assertAllComponentsDoNotExist(components, sstable3);
    }

    private void assertAllComponentsExist(Set<Component> components, SSTableReader sstable)
    {
        for(Component component : components)
        {
            assertTrue(new File(sstable.descriptor.filenameFor(component)).exists());
        }
    }

    private void assertAllComponentsDoNotExist(Set<Component> components, SSTableReader sstable)
    {
        assertTrue(SSTable.componentsFor(sstable.descriptor).isEmpty());
        for(Component component : components)
        {
            assertFalse(new File(sstable.descriptor.filenameFor(component)).exists());
        }
    }
}
