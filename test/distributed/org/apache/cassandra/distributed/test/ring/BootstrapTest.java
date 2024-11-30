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

package org.apache.cassandra.distributed.test.ring;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.JMXUtil;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.utils.Closeable;

import static java.util.Arrays.asList;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.action.GossipHelper.bootstrap;
import static org.apache.cassandra.distributed.action.GossipHelper.pullSchemaFrom;
import static org.apache.cassandra.distributed.action.GossipHelper.statusToBootstrap;
import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BootstrapTest extends TestBaseImpl
{
    private long savedMigrationDelay;

    @Before
    public void beforeTest()
    {
        // MigrationCoordinator schedules schema pull requests immediatelly when the node is just starting up, otherwise
        // the first pull request is sent in 60 seconds. Whether we are starting up or not is detected by examining
        // the node up-time and if it is lower than MIGRATION_DELAY, we consider the server is starting up.
        // When we are running multiple test cases in the class, where each starts a node but in the same JVM, the
        // up-time will be more or less relevant only for the first test. In order to enforce the startup-like behaviour
        // for each test case, the MIGRATION_DELAY time is adjusted accordingly
        savedMigrationDelay = CassandraRelevantProperties.MIGRATION_DELAY.getLong();
        CassandraRelevantProperties.MIGRATION_DELAY.setLong(ManagementFactory.getRuntimeMXBean().getUptime() + savedMigrationDelay);
    }

    @After
    public void afterTest()
    {
        CassandraRelevantProperties.MIGRATION_DELAY.setLong(savedMigrationDelay);
    }

    @Test
    public void bootstrapTest() throws Throwable
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            populate(cluster, 0, 100);

            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance newInstance = cluster.bootstrap(config);
            withProperty("cassandra.join_ring", false,
                         () -> newInstance.startup(cluster));

            cluster.forEach(statusToBootstrap(newInstance));

            cluster.run(asList(pullSchemaFrom(cluster.get(1)),
                               bootstrap()),
                        newInstance.config().num());

            for (Map.Entry<Integer, Long> e : count(cluster).entrySet())
                Assert.assertEquals("Node " + e.getKey() + " has incorrect row state",
                                    100L,
                                    e.getValue().longValue());
        }
    }

    @Test
    public void readWriteDuringBootstrapTest() throws Throwable
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance newInstance = cluster.bootstrap(config);
            withProperty("cassandra.join_ring", false,
                         () -> newInstance.startup(cluster));

            cluster.forEach(statusToBootstrap(newInstance));

            populate(cluster, 0, 100);

            Assert.assertEquals(100, newInstance.executeInternal("SELECT *FROM " + KEYSPACE + ".tbl").length);
        }
    }

    public static void populate(ICluster cluster, int from, int to)
    {
        populate(cluster, from, to, 1, 3, ConsistencyLevel.QUORUM);
    }

    public static void populate(ICluster cluster, int from, int to, int coord, int rf, ConsistencyLevel cl)
    {
        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + rf + "};");
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        for (int i = from; i < to; i++)
        {
            cluster.coordinator(coord).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                               cl,
                                               i, i, i);
        }
    }

    public static Map<Integer, Long> count(ICluster cluster)
    {
        return IntStream.rangeClosed(1, cluster.size())
                        .boxed()
                        .collect(Collectors.toMap(nodeId -> nodeId,
                                                  nodeId -> (Long) cluster.get(nodeId).executeInternal("SELECT count(*) FROM " + KEYSPACE + ".tbl")[0][0]));
    }


    /**
     * This regression test for CASSANDRA-19902 ensures {@link StorageServiceMBean} JMX
     * interface is published before the node finishes bootstrapping
     */
    @Test
    public void testStorageServiceMBeanIsPublishedOnJMXDuringBootstrap() throws Throwable
    {
        ExecutorService es = Executors.newFixedThreadPool(1);
        try (Cluster cluster = builder().withNodes(2)
                                        .withConfig(config -> config.with(GOSSIP)
                                                                    .with(NETWORK)
                                                                    .with(Feature.JMX)
                                                                    .set("auto_bootstrap", true))
                                        .withInstanceInitializer(BBBootstrapInterceptor::install)
                                        .createWithoutStarting();
             Closeable ignored = es::shutdown)
        {
            Runnable test = () ->
            {
                // Wait for bootstrap to start via countdown latch
                IInvokableInstance joiningInstance = cluster.get(2);
                joiningInstance.runOnInstance(() -> Uninterruptibles.awaitUninterruptibly(BBBootstrapInterceptor.bootstrapStart));
                // At this point, it should be possible to check bootstrap status via JMX
                IInstanceConfig config = joiningInstance.config();
                try (JMXConnector jmxc = JMXUtil.getJmxConnector(config))
                {
                    MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
                    StorageServiceMBean sp = JMX.newMBeanProxy(mbsc, new ObjectName("org.apache.cassandra.db:type=StorageService"), StorageServiceMBean.class);
                    assertEquals(sp.getOperationMode(), StorageService.Mode.JOINING.toString());
                }
                catch (IOException | MalformedObjectNameException e)
                {
                    throw new RuntimeException(e);
                }
                finally
                {
                    // Complete bootstrap via countdown latch so test will finish properly
                    joiningInstance.runOnInstance(() -> BBBootstrapInterceptor.bootstrapReady.countDown());
                }
            };

            Future<?> testResult = es.submit(test);
            try
            {
                cluster.startup();
            }
            catch (Exception ex) {
                // ignore exceptions from startup process. More interested in the test result.
            }
            testResult.get();
        }
        es.awaitTermination(5, TimeUnit.SECONDS);
    }

    public static class BBBootstrapInterceptor
    {
        final static CountDownLatch bootstrapReady = new CountDownLatch(1);
        final static CountDownLatch bootstrapStart = new CountDownLatch(1);
        static void install(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber != 2)
                return;
            new ByteBuddy().rebase(StorageService.class)
                           .method(named("bootstrap").and(takesArguments(2)))
                           .intercept(MethodDelegation.to(BBBootstrapInterceptor.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static boolean bootstrap(Collection<Token> tokens, long bootstrapTimeoutMillis)
        {
            bootstrapStart.countDown();
            Uninterruptibles.awaitUninterruptibly(bootstrapReady);
            return false; // bootstrap fails
        }
    }

}
