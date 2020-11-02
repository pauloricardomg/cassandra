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

package org.apache.cassandra.tools;

import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.tokenallocator.TokenAllocation;
import org.apache.cassandra.dht.tokenallocator.TokenAllocator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

public class GenerateTokens
{
    private static final String RF = "rf";
    private static final String TOKENS = "tokens";
    private static final String NODES = "nodes";
    private static final String PARTITIONER = "partitioner";
    private static final String RACKS = "racks";

    public static void main(String[] args) throws ParseException
    {
        Options options = null;
        int rf = 0;
        int tokens = 0;
        int nodes = 0;
        IPartitioner partitioner = null;
        int[] racksDef = null;

        try
        {
            Util.initDatabaseDescriptor();
            options = getOptions();
            CommandLine cmd = parseCommandLine(args, options);

            rf = Integer.parseInt(cmd.getOptionValue(RF));
            tokens = Integer.parseInt(cmd.getOptionValue(TOKENS));
            nodes = Integer.parseInt(cmd.getOptionValue(NODES));

            partitioner = FBUtilities.newPartitioner(cmd.getOptionValue(PARTITIONER, Murmur3Partitioner.class.getSimpleName()));

            racksDef = getRacks(cmd.getOptionValue(RACKS, cmd.getOptionValue(NODES)));
            if (Arrays.stream(racksDef).sum() != nodes)
            {
                throw new AssertionError(String.format("The sum of nodes in each rack %s must equal total node count %s.",
                                                       cmd.getOptionValue(RACKS),
                                                       cmd.getOptionValue(NODES)));
            }
        }
        catch (NumberFormatException e)
        {
            System.err.println("Invalid integer " + e.getMessage());
            System.out.println();
            printUsage(options);
            System.exit(1);
        }
        catch (AssertionError | ConfigurationException | ParseException t)
        {
            System.err.println(t.getMessage());
            System.out.println();
            printUsage(options);
            System.exit(1);
        }

        try
        {
            System.out.println(String.format("Generating tokens for %d nodes with %d vnodes each for replication factor %d and partitioner %s",
                                             nodes, tokens, rf, partitioner.getClass().getSimpleName()));

            int[] idToRack = makeIdToRackMap(nodes, racksDef);
            System.out.println("Node to rack map: " + Arrays.toString(idToRack));

            TokenAllocator<Integer> allocator = TokenAllocation.createTokenGenerator(rf, idToRack, partitioner);
            for (int i = 0; i < nodes; ++i)
            {
                Collection<Token> allocatedTokens = allocator.addUnit(i, tokens);
                if (racksDef.length > 1)
                    System.out.println(String.format("Node %d rack %d:", i, idToRack[i]));
                System.out.println(allocatedTokens);
            }
        }
        catch (Throwable t)
        {
            System.err.println("Error running tool: ");
            t.printStackTrace();
            System.exit(1);
        }
    }

    private static int[] makeIdToRackMap(int nodes, int[] racksDef)
    {
        // Distribute nodes among the racks in round-robin fashion in the order the user is supposed to start them.
        int[] idToRack;
        idToRack = new int[nodes];
        int rack = 0;
        for (int pos = 0; pos < nodes; pos++)
        {
            while (racksDef[rack] == 0)
                if (++rack == racksDef.length)
                    rack = 0;

            idToRack[pos] = rack;
            --racksDef[rack];

            if (++rack == racksDef.length)
                rack = 0;
        }
        return idToRack;
    }

    private static int[] getRacks(String racksDef)
    {
        return Arrays.stream(racksDef.split(",")).mapToInt(Integer::parseInt).toArray();
    }

    private static CommandLine parseCommandLine(String[] args, Options options) throws ParseException
    {
        CommandLineParser parser = new GnuParser();

        CommandLine cmd = parser.parse(options, args, false);

        return cmd;
    }

    private static Options getOptions()
    {
        Options options = new Options();
        options.addOption(requiredOption("n", NODES, true, "Number of nodes."));
        options.addOption(requiredOption("t", TOKENS, true, "Number of tokens/vnodes per node."));
        options.addOption(requiredOption(null, RF, true, "Replication factor."));
        options.addOption(null, PARTITIONER, true, "Database partitioner, either Murmur3Partitioner or RandomPartitioner.");
        options.addOption(null, RACKS, true,
                          "Number of nodes per rack, separated by commas. Must add up to the total node count.\n" +
                          "For example, 'generate-tokens -n 30 -t 8 --rf 3 --racks 10,10,10' will generate tokens for\n" +
                          "three racks of 10 nodes each.");
        return options;
    }

    private static Option requiredOption(String shortOpt, String longOpt, boolean hasArg, String description)
    {
        Option option = new Option(shortOpt, longOpt, hasArg, description);
        option.setRequired(true);
        return option;
    }

    public static void printUsage(Options options)
    {
        String usage = "generate-tokens -n NODES -t TOKENS --rf REPLICATION_FACTOR";
        String header = "--\n" +
                        "Generates tokens for a datacenter with the given number of nodes using the token allocation algorithm.\n" +
                        "Options are:";
        new HelpFormatter().printHelp(usage, header, options, "");
    }
}

