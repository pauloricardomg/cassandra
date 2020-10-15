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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.impl.InstanceConfig;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.service.StorageService;
import org.assertj.core.api.Assertions;

public class OnClusterCrashHostReplaceDeadNodeTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(OnClusterCrashHostReplaceDeadNodeTest.class);

    @Test
    public void test() throws IOException, InterruptedException
    {
        // Gossip has a notiion of quarantine, which is used to remove "fat clients" and "gossip only members"
        // from the ring if not updated recently (recently is defined by this config).
        // The reason for setting to 0 is to make sure even under such an aggressive environment, we do NOT remove
        // nodes from the peers table
        System.setProperty("cassandra.gossip_quarantine_delay", "0");
        // start with 2 nodes, stop both nodes, start the seed, host replace the down node)
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(2);
        try (Cluster cluster = Cluster.build(2)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .withTokenSupplier(node -> even.token(node == 3 ? 2 : node))
                                      .start())
        {
            // call early as this can't be touched on a down node
            InetSocketAddress addressToReplace = cluster.get(2).broadcastAddress();

            // the default is RF=1 which is problematic when bootstraping with downed nodes, so increase the RF
            // so node3 can still bootstrap
            cluster.schemaChange("ALTER KEYSPACE system_auth WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': " + Math.min(cluster.size(), 3) + "}");
            init(cluster);
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int PRIMARY KEY)");

            populate(cluster);
            cluster.forEach(i -> i.flush(KEYSPACE));

            // collect rows/tokens to detect issues later on if the state doesn't match
            SimpleQueryResult expectedState = cluster.coordinator(2).executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
            List<String> beforeCrashTokens = collectTokens(cluster.get(1));

            // now stop all nodes
            stopAll(cluster);

            // with all nodes down, now start the seed (should be first node)
            cluster.get(1).startup();

            // at this point node2 should be known in gossip, but with generation/version of 0
            assertGossipInfo(cluster.get(1), addressToReplace, 0, 0);

            // make sure node1 still has node2's tokens
            List<String> currentTokens = collectTokens(cluster.get(1));
            Assertions.assertThat(currentTokens)
                      .as("Tokens no longer match after restarting")
                      .isEqualTo(beforeCrashTokens);

            // now create a new node to replace the other node
            InstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", true);
            //TODO host replacements should be easier
            config.networkTopology().put(config.broadcastAddress(), NetworkTopology.dcAndRack("datacenter0", "rack0"));
            System.setProperty("cassandra.replace_address_first_boot", addressToReplace.getAddress().getHostAddress());
            //TODO why do we sleep rather than monitor gossip for changes like we do on startup?

            // lower this so the replacement waits less time
            System.setProperty("cassandra.broadcast_interval_ms", Long.toString(TimeUnit.SECONDS.toMillis(30)));
            // default is 30s, lowering as it should be faster
            System.setProperty("cassandra.ring_delay_ms", Long.toString(TimeUnit.SECONDS.toMillis(10)));
            IInvokableInstance node3 = cluster.bootstrap(config);
            node3.startup();

            awaitJoinRing(cluster.get(1), node3);
            awaitJoinRing(node3, cluster.get(1));
            assertNotInRing(cluster.get(1), cluster.get(2));
            assertNotInRing(node3, cluster.get(2));

            expectedState.reset();
            validateRows(cluster.coordinator(1), expectedState);
            expectedState.reset();
            validateRows(cluster.coordinator(3), expectedState);
        }
    }

    List<String> collectTokens(IInvokableInstance inst)
    {
        return inst.callOnInstance(() ->
                                   StorageService.instance.getTokenMetadata()
                                                          .sortedTokens().stream()
                                                          .map(Object::toString)
                                                          .collect(Collectors.toList()));
    }

    void validateRows(ICoordinator coordinator, SimpleQueryResult expected)
    {
        SimpleQueryResult rows = coordinator.executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
        AssertUtils.assertRows(rows, expected);
    }

    void populate(Cluster cluster)
    {
        for (int i = 0; i < 10; i++)
        {
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk) VALUES (?)",
                                           ConsistencyLevel.ALL,
                                           i);
        }
    }

    void stopAll(Cluster cluster)
    {
        cluster.forEach(i -> {
            try
            {
                i.shutdown().get();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            catch (ExecutionException e)
            {
                throw new RuntimeException(e.getCause());
            }
        });
    }

    private static void assertGossipInfo(IInvokableInstance inst,
                                         InetSocketAddress target, int expectedGeneration, int expectedVersion)
    {
        String targetAddress = target.getAddress().toString();
        Table<String, String, String> gossipInfo = gossipInfo(inst);
        Map<String, String> gossipState = gossipInfo.rowMap().get(targetAddress);
        if (gossipState == null)
            throw new NullPointerException("Unable to find gossip info for " + targetAddress + "; gossip info = " + gossipInfo);
        Assert.assertEquals(Long.toString(expectedGeneration), gossipState.get("generation"));
        Assert.assertEquals(Long.toString(expectedVersion), gossipState.get("heartbeat")); //TODO do we really mix these two?
    }

    private static Table<String, String, String> gossipInfo(IInvokableInstance inst)
    {
        NodeToolResult results = inst.nodetoolResult("gossipinfo");
        results.asserts().success();
        return parseGossipInfo(results.getStdout());
    }

    private static List<RingInstanceDetails> ring(IInvokableInstance inst)
    {
        NodeToolResult results = inst.nodetoolResult("ring");
        results.asserts().success();
        return parseRing(results.getStdout());
    }

    private static final class RingInstanceDetails
    {
        private final String address;
        private final String status;
        private final String state;
        private final String token;

        private RingInstanceDetails(String address, String status, String state, String token)
        {
            this.address = address;
            this.status = status;
            this.state = state;
            this.token = token;
        }

        public String toString()
        {
            return Arrays.asList(address, status, state, token).toString();
        }
    }

    private static List<RingInstanceDetails> parseRing(String str)
    {
        // 127.0.0.3  rack0       Up     Normal  46.21 KB        100.00%             -1
        Pattern pattern = Pattern.compile("^([0-9.]+)\\s+\\w+\\s+(\\w+)\\s+(\\w+).*?(-?\\d+)\\s*$");
        List<RingInstanceDetails> details = new ArrayList<>();
        String[] lines = str.split("\n");
        for (String line : lines)
        {
            Matcher matcher = pattern.matcher(line);
            if (!matcher.find())
            {
                continue;
            }
            details.add(new RingInstanceDetails(matcher.group(1), matcher.group(2), matcher.group(3), matcher.group(4)));
        }

        return details;
    }

    private static Table<String, String, String> parseGossipInfo(String str)
    {
        Table<String, String, String> table = HashBasedTable.create();
        String[] lines = str.split("\n");
        String currentInstance = null;
        for (String line : lines)
        {
            if (line.startsWith("/"))
            {
                // start of new instance
                currentInstance = line;
                continue;
            }
            Objects.requireNonNull(currentInstance);
            String[] kv = line.trim().split(":", 2);
            assert kv.length == 2 : "When splitting line " + line + " expected 2 parts but not true";
            table.put(currentInstance, kv[0], kv[1]);
        }

        return table;
    }

    private void assertNotInRing(IInvokableInstance src, IInvokableInstance target)
    {
        String targetAddress = target.config().broadcastAddress().getAddress().getHostAddress();
        List<RingInstanceDetails> ring = ring(src);
        Optional<RingInstanceDetails> match = ring.stream().filter(d -> d.address.equals(targetAddress)).findFirst();
        Assert.assertEquals("Not expected to find " + targetAddress + " but was found", Optional.empty(), match);
    }

    void awaitJoinRing(IInvokableInstance src, IInstance target) throws InterruptedException
    {
        String targetAddress = target.broadcastAddress().getAddress().getHostAddress();
        for (int i = 0; i < 100; i++)
        {
            List<RingInstanceDetails> ring = ring(src);
            logger.info("Got ring for {}: {}", src.broadcastAddress(), ring);
            Optional<RingInstanceDetails> match = ring.stream().filter(d -> d.address.equals(targetAddress)).findFirst();
            if (match.isPresent())
            {
                RingInstanceDetails details = match.get();
                if (details.status.equals("Up") && details.state.equals("Normal"))
                    return;
            }
            TimeUnit.SECONDS.sleep(1);
        }
        throw new AssertionError("Node " + target.broadcastAddress() + " did not join the ring...");
    }
}
