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

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.service.tasks.table.FlushTask;
import org.apache.cassandra.service.tasks.table.TableTask;

enum TaskType
{
    FLUSH(FlushTask.class),
    COMPACT(FlushTask.class),
    SNAPSHOT(FlushTask.class),
    CLEANUP(FlushTask.class);

    final Class<TableTask> taskClass;

    <T extends TableTask> TaskType(Class<T> taskClass)
    {
        this.taskClass = (Class<TableTask>) taskClass;
    }

    public TableTask create(ColumnFamilyStore table)
    {
        try
        {
            return (TableTask) taskClass.getDeclaredConstructors()[0].newInstance(table);
        }
        catch (Exception e)
        {
            throw new RuntimeException(String.format("Unexpected error while creating %s task on %s.%s.", this, table.keyspace.getName(), table.getTableName()));
        }
    }
}