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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.shared.ClusterUtils.assertInRing;
import static org.apache.cassandra.distributed.shared.ClusterUtils.assertRingIs;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitHealthyRing;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitJoinRing;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getTokenMetadataTokens;
import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;

public class HostReplacementTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(HostReplacementTest.class);

    /**
     * Attempt to do a host replacement on a alive host
     */
    @Test
    public void replaceDownedHost() throws IOException, InterruptedException, ExecutionException
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
            IInvokableInstance seed = cluster.get(1);
            IInvokableInstance nodeToRemove = cluster.get(2);

            setupCluster(cluster);

            // collect rows to detect issues later on if the state doesn't match
            SimpleQueryResult expectedState = nodeToRemove.coordinator().executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);

            nodeToRemove.shutdown().get();

            // now create a new node to replace the other node
            IInvokableInstance replacingNode = replaceHostAndStart(cluster, nodeToRemove);

            // wait till the replacing node is in the ring
            awaitJoinRing(seed, replacingNode);
            awaitJoinRing(replacingNode, seed);

            // make sure all nodes are healthy
            awaitHealthyRing(seed);

            assertRingIs(seed, seed, replacingNode);
            logger.info("Current ring is {}", assertRingIs(replacingNode, seed, replacingNode));

            validateRows(seed.coordinator(), expectedState);
            validateRows(replacingNode.coordinator(), expectedState);
        }
        finally
        {
            System.getProperties().remove("cassandra.gossip_quarantine_delay");
        }
    }

    /**
     * Attempt to do a host replacement on a alive host
     */
    @Test
    public void replaceAliveHost() throws IOException, InterruptedException
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
            IInvokableInstance seed = cluster.get(1);
            IInvokableInstance nodeToRemove = cluster.get(2);

            setupCluster(cluster);

            // collect rows to detect issues later on if the state doesn't match
            SimpleQueryResult expectedState = nodeToRemove.coordinator().executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);

            // now create a new node to replace the other node
            Assertions.assertThatThrownBy(() -> replaceHostAndStart(cluster, nodeToRemove))
                      .as("Startup of instance should have failed as you can not replace a alive node")
                      .hasMessageContaining("Cannot replace a live node")
                      .isInstanceOf(UnsupportedOperationException.class);

            // make sure all nodes are healthy
            awaitHealthyRing(seed);

            assertRingIs(seed, seed, nodeToRemove);
            logger.info("Current ring is {}", assertRingIs(nodeToRemove, seed, nodeToRemove));

            validateRows(seed.coordinator(), expectedState);
            validateRows(nodeToRemove.coordinator(), expectedState);
        }
        finally
        {
            System.getProperties().remove("cassandra.gossip_quarantine_delay");
        }
    }

    /**
     * If the seed goes down, then another node, once the seed comes back, make sure host replacements still work.
     */
    @Test
    public void seedGoesDownBeforeDownHost() throws IOException, ExecutionException, InterruptedException
    {
        // Gossip has a notiion of quarantine, which is used to remove "fat clients" and "gossip only members"
        // from the ring if not updated recently (recently is defined by this config).
        // The reason for setting to 0 is to make sure even under such an aggressive environment, we do NOT remove
        // nodes from the peers table
        System.setProperty("cassandra.gossip_quarantine_delay", "0");
        // start with 3 nodes, stop both nodes, start the seed, host replace the down node)
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3);
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .withTokenSupplier(node -> even.token(node == 4 ? 2 : node))
                                      .start())
        {
            // call early as this can't be touched on a down node
            IInvokableInstance seed = cluster.get(1);
            IInvokableInstance nodeToRemove = cluster.get(2);
            IInvokableInstance nodeToStayAlive = cluster.get(3);

            setupCluster(cluster);

            // collect rows/tokens to detect issues later on if the state doesn't match
            SimpleQueryResult expectedState = nodeToRemove.coordinator().executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
            List<String> beforeCrashTokens = getTokenMetadataTokens(seed);

            // shutdown the seed, then the node to remove
            seed.shutdown().get();
            nodeToRemove.shutdown().get();

            // restart the seed
            seed.startup();

            // make sure the node to remove is still in the ring
            assertInRing(seed, nodeToRemove);

            // make sure node1 still has node2's tokens
            List<String> currentTokens = getTokenMetadataTokens(seed);
            Assertions.assertThat(currentTokens)
                      .as("Tokens no longer match after restarting")
                      .isEqualTo(beforeCrashTokens);

            // now create a new node to replace the other node
            IInvokableInstance replacingNode = replaceHostAndStart(cluster, nodeToRemove);

            List<IInvokableInstance> expectedRing = Arrays.asList(seed, replacingNode, nodeToStayAlive);

            // wait till the replacing node is in the ring
            awaitJoinRing(seed, replacingNode);
            awaitJoinRing(replacingNode, seed);
            awaitJoinRing(nodeToStayAlive, replacingNode);

            // make sure all nodes are healthy
            logger.info("Current ring is {}", awaitHealthyRing(seed));

            expectedRing.forEach(i -> assertRingIs(i, expectedRing));

            validateRows(seed.coordinator(), expectedState);
            validateRows(replacingNode.coordinator(), expectedState);
        }
        finally
        {
            System.getProperties().remove("cassandra.gossip_quarantine_delay");
        }
    }

    private void setupCluster(Cluster cluster)
    {
        fixDistributedSchemas(cluster);
        init(cluster);

        populate(cluster);
        cluster.forEach(i -> i.flush(KEYSPACE));
    }

    void populate(Cluster cluster)
    {
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int PRIMARY KEY)");
        for (int i = 0; i < 10; i++)
        {
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk) VALUES (?)",
                                           ConsistencyLevel.ALL,
                                           i);
        }
    }

    void validateRows(ICoordinator coordinator, SimpleQueryResult expected)
    {
        expected.reset();
        SimpleQueryResult rows = coordinator.executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
        AssertUtils.assertRows(rows, expected);
    }
}
