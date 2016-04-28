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

import java.util.ArrayList;
import java.util.List;

import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "mutationbasedrepair", description = "Slowly repair the data on this node")
public class MutationBasedRepair extends NodeTool.NodeToolCmd
{
    @Arguments(usage = "<keyspace> <table>", description = "The keyspace and table name", required = true)
    private List<String> args = new ArrayList<>();

    @Option(title = "windowsize",
            name = {"-w", "--windowsize"},
            description = "Size of the repair window - this many rows are compared with the replicas at a time")
    private int windowSize = 500;
    @Option(title = "rowsPerSecondToRepair",
            name = {"-r", "--rate"},
            description = "Number of rows to repair per second")
    private int rowsPerSecond = 2000;

    @Override
    public void execute(NodeProbe probe)
    {
        assert args.size() == 2 : "You need to state keyspace and table";
        String keyspace = args.get(0);
        String table = args.get(1);
        try
        {
            probe.enableMutationBasedRepair(keyspace, table, windowSize, rowsPerSecond);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Got error while repairing", e);
        }
    }
}
