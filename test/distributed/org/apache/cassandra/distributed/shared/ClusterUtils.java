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

package org.apache.cassandra.distributed.shared;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.Futures;
import org.junit.Assert;

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.impl.InstanceConfig;
import org.apache.cassandra.service.StorageService;
import org.assertj.core.api.Assertions;

/**
 * Utilities for working with jvm-dtest clusters.
 *
 * This class is marked as Isolated as it relies on lambdas, which are in a package that is marked as shared, so need to
 * tell jvm-dtest to not share this class.
 *
 * This class should never be called from within the cluster, always in the App ClassLoader.
 */
@Isolated
public class ClusterUtils
{
    /**
     * Start the instance with the given System Properties, after the instance has started, the properties will be cleared.
     */
    public static <I extends IInstance> I start(I inst, Consumer<WithProperties> fn)
    {
        return start(inst, (ignore, prop) -> fn.accept(prop));
    }

    /**
     * Start the instance with the given System Properties, after the instance has started, the properties will be cleared.
     */
    public static <I extends IInstance> I start(I inst, BiConsumer<I, WithProperties> fn)
    {
        try (WithProperties properties = new WithProperties())
        {
            fn.accept(inst, properties);
            inst.startup();
            return inst;
        }
    }

    /**
     * Stop an instance in a blocking manner.
     *
     * The main difference between this and {@link IInstance#shutdown()} is that the wait on the future will catch
     * the exceptions and throw as runtime.
     */
    public static void stopUnchecked(IInstance i)
    {
        Futures.getUnchecked(i.shutdown());
    }

    /**
     * Stop all the instances in the cluster.  This function is differe than {@link ICluster#close()} as it doesn't
     * clean up the cluster state, it only stops all the instances.
     */
    public static <I extends IInstance> void stopAll(ICluster<I> cluster)
    {
        cluster.stream().forEach(ClusterUtils::stopUnchecked);
    }

    /**
     * Create a new instance and add it to the cluster, without starting it.
     *
     * @param cluster to add to
     * @param dc the instance should be in
     * @param rack the instance should be in
     * @param <I> instance type
     * @return the instance added
     */
    public static <I extends IInstance> I addInstance(AbstractCluster<I> cluster,
                                                      String dc, String rack)
    {
        return addInstance(cluster, dc, rack, ignore -> {});
    }

    /**
     * Create a new instance and add it to the cluster, without starting it.
     *
     * @param cluster to add to
     * @param dc the instance should be in
     * @param rack the instance should be in
     * @param fn function to add to the config before starting
     * @param <I> instance type
     * @return the instance added
     */
    public static <I extends IInstance> I addInstance(AbstractCluster<I> cluster,
                                                      String dc, String rack,
                                                      Consumer<IInstanceConfig> fn)
    {
        Objects.requireNonNull(dc, "dc");
        Objects.requireNonNull(rack, "rack");

        InstanceConfig config = cluster.newInstanceConfig();
        //TODO host replacements should be easier
        // this is very hidden, so should be more explicit
        config.networkTopology().put(config.broadcastAddress(), NetworkTopology.dcAndRack(dc, rack));

        fn.accept(config);

        return cluster.bootstrap(config);
    }

    /**
     * Create and start a new instance that replaces an existing instance.
     *
     * The instance will be in the same datacenter and rack as the existing instance.
     *
     * @param cluster to add to
     * @param toReplace instance to replace
     * @param <I> instance type
     * @return the instance added
     */
    public static <I extends IInstance> I replaceHostAndStart(AbstractCluster<I> cluster, IInstance toReplace)
    {
        IInstanceConfig toReplaceConf = toReplace.config();
        I inst = addInstance(cluster, toReplaceConf.localDatacenter(), toReplaceConf.localRack(), c -> c.set("auto_bootstrap", true));

        return start(inst, properties -> {
            // lower this so the replacement waits less time
            properties.setProperty("cassandra.broadcast_interval_ms", Long.toString(TimeUnit.SECONDS.toMillis(30)));
            // default is 30s, lowering as it should be faster
            properties.setProperty("cassandra.ring_delay_ms", Long.toString(TimeUnit.SECONDS.toMillis(10)));

            // state which node to replace
            properties.setProperty("cassandra.replace_address_first_boot", toReplace.config().broadcastAddress().getAddress().getHostAddress());
        });
    }

    /**
     * Calls {@link org.apache.cassandra.locator.TokenMetadata#sortedTokens()}, returning as a list of strings.
     */
    public static List<String> getTokenMetadataTokens(IInvokableInstance inst)
    {
        return inst.callOnInstance(() ->
                                   StorageService.instance.getTokenMetadata()
                                                          .sortedTokens().stream()
                                                          .map(Object::toString)
                                                          .collect(Collectors.toList()));
    }

    /**
     * Get the ring from the perspective of the instance.
     */
    public static List<RingInstanceDetails> ring(IInstance inst)
    {
        NodeToolResult results = inst.nodetoolResult("ring");
        results.asserts().success();
        return parseRing(results.getStdout());
    }

    public static List<RingInstanceDetails> assertInRing(IInstance src, IInstance target)
    {
        String targetAddress = target.config().broadcastAddress().getAddress().getHostAddress();
        List<RingInstanceDetails> ring = ring(src);
        Optional<RingInstanceDetails> match = ring.stream().filter(d -> d.address.equals(targetAddress)).findFirst();
        Assertions.assertThat(match).as("Not expected to find %s but was found", targetAddress).isPresent();
        return ring;
    }

    /**
     * Make sure the target instance is NOT in the ring.
     *
     * @param src instance to check on
     * @param target instance not expected in the ring
     * @return the ring (if target is not present)
     */
    public static List<RingInstanceDetails> assertNotInRing(IInstance src, IInstance target)
    {
        String targetAddress = target.config().broadcastAddress().getAddress().getHostAddress();
        List<RingInstanceDetails> ring = ring(src);
        Optional<RingInstanceDetails> match = ring.stream().filter(d -> d.address.equals(targetAddress)).findFirst();
        Assert.assertEquals("Not expected to find " + targetAddress + " but was found", Optional.empty(), match);
        return ring;
    }

    /**
     * Wait for the target to be in the ring as seen by the source instance.
     *
     * @param src instance to check on
     * @param target instance to wait for
     * @return the ring
     */
    public static List<RingInstanceDetails> awaitJoinRing(IInstance src, IInstance target) throws InterruptedException
    {
        String targetAddress = target.broadcastAddress().getAddress().getHostAddress();
        for (int i = 0; i < 100; i++)
        {
            List<RingInstanceDetails> ring = ring(src);
            Optional<RingInstanceDetails> match = ring.stream().filter(d -> d.address.equals(targetAddress)).findFirst();
            if (match.isPresent())
            {
                RingInstanceDetails details = match.get();
                if (details.status.equals("Up") && details.state.equals("Normal"))
                    return ring;
            }
            TimeUnit.SECONDS.sleep(1);
        }
        throw new AssertionError("Node " + target.broadcastAddress() + " did not join the ring...");
    }

    /**
     * Wait for the ring to only have instances that are Up and Normal.
     *
     * @param src instance to check on
     * @return the ring
     */
    public static List<RingInstanceDetails> awaitHealthyRing(IInstance src) throws InterruptedException
    {
        for (int i = 0; i < 100; i++)
        {
            List<RingInstanceDetails> ring = ring(src);
            if (ring.stream().allMatch(ClusterUtils::isRingInstanceDetailsHealthy))
            {
                // all nodes are healthy
                return ring;
            }
            TimeUnit.SECONDS.sleep(1);
        }
        throw new AssertionError("Timeout waiting for ring to become healthy");
    }

    /**
     * Make sure the ring is only the expected instances.  The source instance may not be in the ring, so this function
     * only relies on the expectedInsts param.
     *
     * @param src instance to check on
     * @param expectedInsts expected instances in the ring
     * @return the ring (if condition is true)
     */
    public static List<RingInstanceDetails> assertRingIs(IInstance src, IInstance... expectedInsts)
    {
        return assertRingIs(src, Arrays.asList(expectedInsts));
    }

    /**
     * Make sure the ring is only the expected instances.  The source instance may not be in the ring, so this function
     * only relies on the expectedInsts param.
     *
     * @param src instance to check on
     * @param expectedInsts expected instances in the ring
     * @return the ring (if condition is true)
     */
    public static List<RingInstanceDetails> assertRingIs(IInstance src, List<? extends IInstance> expectedInsts)
    {
        Set<String> expectedRingAddresses = expectedInsts.stream()
                                                  .map(i -> i.config().broadcastAddress().getAddress().getHostAddress())
                                                  .collect(Collectors.toSet());
        List<RingInstanceDetails> ring = ring(src);
        Set<String> ringAddresses = ring.stream().map(d -> d.address).collect(Collectors.toSet());
        Assertions.assertThat(ringAddresses)
                  .as("Ring addreses did not match")
                  .isEqualTo(expectedRingAddresses);
        return ring;
    }

    private static boolean isRingInstanceDetailsHealthy(RingInstanceDetails details)
    {
        return details.status.equals("Up") && details.state.equals("Normal");
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

    /**
     * Get the gossip information from the node.  Currently only address, generation, and heartbeat are returned
     *
     * @param inst to check on
     * @return gossip info
     */
    public static Table<String, String, String> gossipInfo(IInstance inst)
    {
        NodeToolResult results = inst.nodetoolResult("gossipinfo");
        results.asserts().success();
        return parseGossipInfo(results.getStdout());
    }

    /**
     * Make sure the gossip info for the specific target has the expected generation and heartbeat
     *
     * @param inst to check on
     * @param target instance to check for
     * @param expectedGeneration expected generation
     * @param expectedHeartbeat expected heartbeat
     */
    public static void assertGossipInfo(IInstance inst,
                                        InetSocketAddress target, int expectedGeneration, int expectedHeartbeat)
    {
        String targetAddress = target.getAddress().toString();
        Table<String, String, String> gossipInfo = gossipInfo(inst);
        Map<String, String> gossipState = gossipInfo.rowMap().get(targetAddress);
        if (gossipState == null)
            throw new NullPointerException("Unable to find gossip info for " + targetAddress + "; gossip info = " + gossipInfo);
        Assert.assertEquals(Long.toString(expectedGeneration), gossipState.get("generation"));
        Assert.assertEquals(Long.toString(expectedHeartbeat), gossipState.get("heartbeat")); //TODO do we really mix these two?
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

    public static final class RingInstanceDetails
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

        public String getAddress()
        {
            return address;
        }

        public String getStatus()
        {
            return status;
        }

        public String getState()
        {
            return state;
        }

        public String getToken()
        {
            return token;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RingInstanceDetails that = (RingInstanceDetails) o;
            return Objects.equals(address, that.address) &&
                   Objects.equals(status, that.status) &&
                   Objects.equals(state, that.state) &&
                   Objects.equals(token, that.token);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(address, status, state, token);
        }

        public String toString()
        {
            return Arrays.asList(address, status, state, token).toString();
        }
    }
}
