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

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.collect.Sets;

import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.commons.lang3.StringUtils;

@Command(name = "repair", description = "Repair one or more tables")
public class Repair extends NodeToolCmd
{
    public final static Set<String> ONLY_EXPLICITLY_REPAIRED = Sets.newHashSet(SystemDistributedKeyspace.NAME);

    @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
    private List<String> args = new ArrayList<>();

    @Option(title = "seqential", name = {"-seq", "--sequential"}, description = "Use -seq to carry out a sequential repair")
    private boolean sequential = false;

    @Option(title = "dc parallel", name = {"-dcpar", "--dc-parallel"}, description = "Use -dcpar to repair data centers in parallel.")
    private boolean dcParallel = false;

    @Option(title = "local_dc", name = {"-local", "--in-local-dc"}, description = "Use -local to only repair against nodes in the same datacenter")
    private boolean localDC = false;

    @Option(title = "specific_dc", name = {"-dc", "--in-dc"}, description = "Use -dc to repair specific datacenters")
    private List<String> specificDataCenters = new ArrayList<>();;

    @Option(title = "specific_host", name = {"-hosts", "--in-hosts"}, description = "Use -hosts to repair specific hosts")
    private List<String> specificHosts = new ArrayList<>();

    @Option(title = "start_token", name = {"-st", "--start-token"}, description = "Use -st to specify a token at which the repair range starts")
    private String startToken = EMPTY;

    @Option(title = "end_token", name = {"-et", "--end-token"}, description = "Use -et to specify a token at which repair range ends")
    private String endToken = EMPTY;

    @Option(title = "primary_range", name = {"-pr", "--partitioner-range"}, description = "Use -pr to repair only the first range returned by the partitioner")
    private boolean primaryRange = false;

    @Option(title = "full", name = {"-full", "--full"}, description = "Use -full to issue a full repair.")
    private boolean fullRepair = false;

    @Option(title = "list", name = {"-li", "--list"}, description = "Use --list to list active repair sessions.")
    private boolean listSimple = false;

    @Option(title = "listDetailed", name = {"-ld", "--list-detailed"}, description = "Use --list-detailed to show more details when listing active repair sessions.")
    private boolean listDetailed = false;

    @Option(title = "sessionId", name = {"-a", "--abort"}, description = "Abort repair session with given id")
    private String abortRepairSessionId = EMPTY;

    @Option(title = "job_threads", name = {"-j", "--job-threads"}, description = "Number of threads to run repair jobs. " +
                                                                                 "Usually this means number of CFs to repair concurrently. " +
                                                                                 "WARNING: increasing this puts more load on repairing nodes, so be careful. (default: 1, max: 4)")
    private int numJobThreads = 1;

    @Option(title = "trace_repair", name = {"-tr", "--trace"}, description = "Use -tr to trace the repair. Traces are logged to system_traces.events.")
    private boolean trace = false;

    @Override
    public void execute(NodeProbe probe)
    {
        if (listSimple || listDetailed)
        {
            listRepairs(probe, listDetailed);
            return;
        }

        if (!abortRepairSessionId.equals(EMPTY))
        {
            abortRepair(probe, abortRepairSessionId);
            return;
        }

        List<String> keyspaces = parseOptionalKeyspace(args, probe);
        String[] cfnames = parseOptionalTables(args);

        if (primaryRange && (!specificDataCenters.isEmpty() || !specificHosts.isEmpty()))
            throw new RuntimeException("Primary range repair should be performed on all nodes in the cluster.");

        for (String keyspace : keyspaces)
        {
            // avoid repairing system_distributed by default (CASSANDRA-9621)
            if ((args == null || args.isEmpty()) && ONLY_EXPLICITLY_REPAIRED.contains(keyspace))
                continue;

            Map<String, String> options = new HashMap<>();
            RepairParallelism parallelismDegree = RepairParallelism.PARALLEL;
            if (sequential)
                parallelismDegree = RepairParallelism.SEQUENTIAL;
            else if (dcParallel)
                parallelismDegree = RepairParallelism.DATACENTER_AWARE;
            options.put(RepairOption.PARALLELISM_KEY, parallelismDegree.getName());
            options.put(RepairOption.PRIMARY_RANGE_KEY, Boolean.toString(primaryRange));
            options.put(RepairOption.INCREMENTAL_KEY, Boolean.toString(!fullRepair));
            options.put(RepairOption.JOB_THREADS_KEY, Integer.toString(numJobThreads));
            options.put(RepairOption.TRACE_KEY, Boolean.toString(trace));
            options.put(RepairOption.COLUMNFAMILIES_KEY, StringUtils.join(cfnames, ","));
            if (!startToken.isEmpty() || !endToken.isEmpty())
            {
                options.put(RepairOption.RANGES_KEY, startToken + ":" + endToken);
            }
            if (localDC)
            {
                options.put(RepairOption.DATACENTERS_KEY, StringUtils.join(newArrayList(probe.getDataCenter()), ","));
            }
            else
            {
                options.put(RepairOption.DATACENTERS_KEY, StringUtils.join(specificDataCenters, ","));
            }
            options.put(RepairOption.HOSTS_KEY, StringUtils.join(specificHosts, ","));
            try
            {
                probe.repairAsync(System.out, keyspace, options);
            } catch (Exception e)
            {
                throw new RuntimeException("Error occurred during repair", e);
            }
        }
    }

    private void abortRepair(NodeProbe probe, String parentSessionId)
    {
            if (probe.abortParentRepairSession(parentSessionId))
                System.out.println(String.format("Successfully aborted parent repair session with id %s.", parentSessionId));
            else
                System.out.println(String.format("Did not find repair session with id %s. (maybe try nodetol repair --list?)", parentSessionId));

    }

    private void listRepairs(NodeProbe probe, boolean detailed)
    {
        try
        {
            Map<String, Map<String, String>> runningRepairs = probe.listRepairs();
            if (runningRepairs.isEmpty())
            {
                System.out.println("No active repair sessions on this node.");
            }
            for (Map.Entry<String, Map<String, String>> repairStatuses : runningRepairs.entrySet())
            {
                Map<String, String> infoAsMap = repairStatuses.getValue();
                System.out.printf("Session %s:%n", repairStatuses.getKey());
                System.out.printf("\tCordinator: %s%n", infoAsMap.remove("coordinator"));
                System.out.printf("\tRepair status per table:%n");
                SortedMap<String, SortedMap<String, List<String>>> allTablesStatuses = constructTableStatuses(infoAsMap);
                for (Map.Entry<String, SortedMap<String, List<String>>> entry : allTablesStatuses.entrySet())
                {
                    String table = entry.getKey();
                    System.out.printf("\t\t- %s:%n", table);
                    System.out.printf("\t\t\tActive subtasks: %d%n", getTasksPerTable(allTablesStatuses, table));
                    System.out.printf("\t\t\tSubtasks in each status:%n", table);
                    for (Map.Entry<String, List<String>> tableInfo : entry.getValue().entrySet())
                    {
                        List<String> sessionIds = tableInfo.getValue();
                        if (!sessionIds.isEmpty())
                            System.out.printf("\t\t\t\t- %s: %s%n", tableInfo.getKey(), detailed ? sessionIds : sessionIds.size());
                    }
                    System.out.println("");
                }
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error occurred while listing repairs", e);
        }
    }

    private Integer getTasksPerTable(Map<String, SortedMap<String, List<String>>> statusByTable, String table)
    {
        return statusByTable.get(table).values().stream().mapToInt(l -> l.size()).sum();
    }

    private SortedMap<String, SortedMap<String, List<String>>> constructTableStatuses(Map<String, String> infoAsMap)
    {
        SortedMap<String, SortedMap<String, List<String>>> allTablesStatuses = new TreeMap<>();
        for (Map.Entry<String, String> tableStatus : infoAsMap.entrySet())
        {
            String[] key = tableStatus.getKey().split(":");
            String tableName = key[1];
            String sessionId = key[2];
            String status = tableStatus.getValue();
            SortedMap<String, List<String>> tableStatuses = allTablesStatuses.get(tableName);
            if (tableStatuses == null)
            {
                tableStatuses = new TreeMap<>();
                allTablesStatuses.put(tableName, tableStatuses);
            }
            List<String> sessionIds = tableStatuses.get(status);
            if (sessionIds == null)
            {
                sessionIds = new LinkedList<>();
                tableStatuses.put(status, sessionIds);
            }
            sessionIds.add(sessionId);
        }
        return allTablesStatuses;
    }
}