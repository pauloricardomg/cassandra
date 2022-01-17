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

package org.apache.cassandra.tools.nodetool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.management.openmbean.TabularData;

import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class SnapshotTest extends CQLTester
{
    private static int KEYSPACE_IDX = 1;
    private static int TABLE_IDX = 2;
    private static NodeProbe probe;

    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
        probe = new NodeProbe(jmxHost, jmxPort);
    }

    @AfterClass
    public static void closeNodetool() throws IOException
    {
        probe.close();
    }

    @Before
    public void tearDown()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("clearsnapshot", "--all");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();
    }

    @Test
    public void testSnapshot_singleKeyspace()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("snapshot", "-t", "tag", SchemaConstants.SYSTEM_KEYSPACE_NAME);
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();

        Map<String, TabularData> snapshots = probe.getSnapshotDetails();
        assertThat(snapshots).containsKey("tag");

        Set<String> snapshottedKeyspaces = getRows(snapshots.get("tag")).stream().map(r -> r.get(KEYSPACE_IDX)).collect(Collectors.toSet());
        assertThat(snapshottedKeyspaces).isEqualTo(Sets.newHashSet(SchemaConstants.SYSTEM_KEYSPACE_NAME));
    }

    @Test
    public void testSnapshot_multipleKeyspaces()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("snapshot", "-t", "tag", SchemaConstants.SYSTEM_KEYSPACE_NAME, SchemaConstants.SCHEMA_KEYSPACE_NAME);
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();

        Map<String, TabularData> snapshots = probe.getSnapshotDetails();
        assertThat(snapshots).containsKey("tag");

        Set<String> snapshottedKeyspaces = getRows(snapshots.get("tag")).stream().map(r -> r.get(KEYSPACE_IDX)).collect(Collectors.toSet());
        assertThat(snapshottedKeyspaces).contains(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        assertThat(snapshottedKeyspaces).contains(SchemaConstants.SCHEMA_KEYSPACE_NAME);
    }

    @Test
    public void testSnapshot_allKeyspaces()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("snapshot", "-t", "tag");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();

        Map<String, TabularData> snapshots = probe.getSnapshotDetails();
        assertThat(snapshots).containsKey("tag");

        Set<String> snapshottedKeyspaces = getRows(snapshots.get("tag")).stream().map(r -> r.get(KEYSPACE_IDX)).collect(Collectors.toSet());
        assertThat(snapshottedKeyspaces).contains(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        assertThat(snapshottedKeyspaces).contains(SchemaConstants.SCHEMA_KEYSPACE_NAME);
    }

    @Test
    public void testSnapshot_singleTable()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("snapshot", "-t", "tag", "--table", SchemaKeyspaceTables.TABLES, SchemaConstants.SCHEMA_KEYSPACE_NAME);
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();

        Map<String, TabularData> snapshots = probe.getSnapshotDetails();
        assertThat(snapshots).containsKey("tag");

        Set<String> snapshottedTables = getRows(snapshots.get("tag")).stream().map(r -> String.format("%s.%s", r.get(KEYSPACE_IDX), r.get(TABLE_IDX))).collect(Collectors.toSet());
        assertThat(snapshottedTables).isEqualTo(Sets.newHashSet(String.format("%s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.TABLES)));
    }

    @Test
    public void testSnapshot_multipleTables()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("snapshot", "-t", "tag", "-kt", String.format("%s.%s,%s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.TABLES,
                                                                                                                                   SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.KEYSPACES));
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();

        Map<String, TabularData> snapshots = probe.getSnapshotDetails();
        assertThat(snapshots).containsKey("tag");

        Set<String> snapshottedTables = getRows(snapshots.get("tag")).stream().map(r -> String.format("%s.%s", r.get(KEYSPACE_IDX), r.get(TABLE_IDX))).collect(Collectors.toSet());
        assertThat(snapshottedTables).hasSize(2);
        assertThat(snapshottedTables).contains(String.format("%s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.KEYSPACES));
        assertThat(snapshottedTables).contains(String.format("%s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.TABLES));
    }

    public List<List<String>> getRows(TabularData tabularData)
    {
        Set<?> values = tabularData.keySet();
        List<List<String>> rows = new ArrayList<>(values.size());
        for (Object row : values)
        {
            List<?> fields = (List<?>) row;
            rows.add(fields.stream().map(r -> (String)r).collect(Collectors.toList()));
        }
        return rows;
    }
}
