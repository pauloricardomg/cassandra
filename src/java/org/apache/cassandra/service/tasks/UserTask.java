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

package org.apache.cassandra.service.tasks;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.tasks.table.TableTask;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class UserTask
{
    private static final String TABLE_SEPARATOR = ".";

    private final UUID id;
    private final TaskType type;
    private final Set<TableTask> tasks;

    public UserTask(UUID id, TaskType type, Set<TableTask> tasks)
    {
        this.id = id;
        this.type = type;
        this.tasks = tasks;
    }

    Future<?> start()
    {
        Future<?> result = ImmediateFuture.success(null);

        for (TableTask task : tasks)
        {
            result = result.flatMap(o -> task.start());
        }

        return result;
    }

    public static UserTask create(TaskType type, TaskParams params, String... tableList)
    {
        UserTaskBuilder builder = new UserTaskBuilder(type);

        for (String ksTable : tableList)
        {
            String[] split = ksTable.split(TABLE_SEPARATOR);
            assert split.length >= 1 && split.length <= 2;
            String keyspaceName = split[0];
            if (split.length > 1)
            {
                builder.onKeyspace(keyspaceName);
            }
            else
            {
                String tableName = split[0];
                builder.onTable(keyspaceName, tableName);
            }
        }

        return builder.build();
    }
    protected static class UserTaskBuilder
    {
        private final UUID id = nextTimeUUID().asUUID();

        private final Set<TableTask> tasks = new LinkedHashSet<>();
        private final TaskType type;

        public UserTaskBuilder(TaskType type) {
            this.type = type;
        }

        public UserTaskBuilder onKeyspace(String keyspaceName)
        {
            getKeyspace(keyspaceName).getValidColumnFamilies(true, false)
                                     .forEach(t -> tasks.add(type.create(t)));
            return this;
        }

        public void onTable(String keyspace, String table)
        {
            tasks.add(type.create(getKeyspace(keyspace).getColumnFamilyStore(table)));
        }

        public UserTask build()
        {
            return new UserTask(id, type, tasks);
        }

        private static Keyspace getKeyspace(String keyspaceName)
        {
            if (null != VirtualKeyspaceRegistry.instance.getKeyspaceNullable(keyspaceName))
                throw new IllegalArgumentException("Cannot perform any operations against virtual keyspace " + keyspaceName);

            Keyspace keyspace = Schema.instance.getKeyspaceInstance(keyspaceName);
            if (keyspace == null)
                throw new IllegalArgumentException("Keyspace " + keyspaceName + " does not exist");
            return keyspace;
        }
    }
}
