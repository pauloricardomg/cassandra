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

package org.apache.cassandra.ring;

import java.util.Arrays;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.UUIDGen;

import static org.assertj.core.api.Assertions.assertThat;

public class MultiDatacenterRingTest
{
    static final UUID NODE_A = UUIDGen.getTimeUUID();
    static final UUID NODE_B = UUIDGen.getTimeUUID();
    static final UUID NODE_C = UUIDGen.getTimeUUID();
    static final UUID NODE_D = UUIDGen.getTimeUUID();
    static final UUID NODE_E = UUIDGen.getTimeUUID();

    @Test
    public void testgetReplicasForTokenWrite_1dc_1rack_5nodes_rf3()
    {
        // Node's Tokens
        // | A: [0, 150, 650]    |  C: [200, 350, 850]  |  E: [400, 550, 1050]
        // | B: [100, 250, 750]  |  D: [300, 450, 950]  |
        TestCluster cluster = TestCluster.builder()
                                         .withDatacenter("dc1").withReplicationFactor(3)
                                         .withNode(NODE_A).withManualTokens(0L, 150L, 650L)
                                         .withNode(NODE_B).withManualTokens(100L, 250L, 750L)
                                         .withNode(NODE_C).withManualTokens(200L, 350L, 850L)
                                         .withNode(NODE_D).withManualTokens(300L, 450L, 950L)
                                         .withNode(NODE_E).withManualTokens(400L, 550L, 1050L)
                                         .build();

        // RING LAYOUT
        // <--[0:A]--[100:B]--[150:A]--[200:C]--[250:B]--[300:D]--[350:C]-->
        // <--[400:E]--[450:D]--[550:E]--[650:A]--[750:B]--[850:C]--[950:D]--[1050:E]-->
        MultiDatacenterRing ring = cluster.getRing();

        // Token 10: [100:B]--[150:A]--[200:C]
        assertThat(ring.getReplicasForTokenWrite(token(10L))).isEqualTo(normalReplicas(NODE_B, NODE_A, NODE_C));

        // Token 110: [150:A]--[200:C]--[250:B]
        assertThat(ring.getReplicasForTokenWrite(token(110L))).isEqualTo(normalReplicas(NODE_A, NODE_C, NODE_B));

        // Token 160: [200:C]--[250:B]--[300:D]
        assertThat(ring.getReplicasForTokenWrite(token(160L))).isEqualTo(normalReplicas(NODE_C, NODE_B, NODE_D));

        // Token 210: [250:B]--[300:D]--[350:C]
        assertThat(ring.getReplicasForTokenWrite(token(210L))).isEqualTo(normalReplicas(NODE_B, NODE_D, NODE_C));

        // Token 260: [300:D]--[350:C]--[400:E]
        assertThat(ring.getReplicasForTokenWrite(token(260L))).isEqualTo(normalReplicas(NODE_D, NODE_C, NODE_E));

        // Token 310: [350:C]--[400:E]--[450:D]
        assertThat(ring.getReplicasForTokenWrite(token(310L))).isEqualTo(normalReplicas(NODE_C, NODE_E, NODE_D));

        // Token 360: [400:E]--[450:D]--[550:E]*--[650:A] (* token [550:E] is skipped because node E already replicates this range)
        assertThat(ring.getReplicasForTokenWrite(token(360L))).isEqualTo(normalReplicas(NODE_E, NODE_D, NODE_A));

        // Token 410: [450:D]--[550:E]--[650:A]
        assertThat(ring.getReplicasForTokenWrite(token(410L))).isEqualTo(normalReplicas(NODE_D, NODE_E, NODE_A));

        // Token 460: [550:E]--[650:A]--[750:B]
        assertThat(ring.getReplicasForTokenWrite(token(460L))).isEqualTo(normalReplicas(NODE_E, NODE_A, NODE_B));

        // Token 560: [650:A]--[750:B]--[850:C]
        assertThat(ring.getReplicasForTokenWrite(token(560L))).isEqualTo(normalReplicas(NODE_A, NODE_B, NODE_C));

        // Token 660: [750:B]--[850:C]--[950:D]
        assertThat(ring.getReplicasForTokenWrite(token(660L))).isEqualTo(normalReplicas(NODE_B, NODE_C, NODE_D));

        // Token 760: [850:C]--[950:D]--[1050:E]
        assertThat(ring.getReplicasForTokenWrite(token(760L))).isEqualTo(normalReplicas(NODE_C, NODE_D, NODE_E));

        // Token 860: [950:D]--[1050:E]--[0:A]
        assertThat(ring.getReplicasForTokenWrite(token(860L))).isEqualTo(normalReplicas(NODE_D, NODE_E, NODE_A));

        // Token 960: [1050:E]--[0:A]--[100:B]
        assertThat(ring.getReplicasForTokenWrite(token(960L))).isEqualTo(normalReplicas(NODE_E, NODE_A, NODE_B));

        // Token 1060: [0:A]--[100:B]--[150:A]*--[200:C] (* token [150:A] is skipped because node A already replicates this range)
        assertThat(ring.getReplicasForTokenWrite(token(1060L))).isEqualTo(normalReplicas(NODE_A, NODE_B, NODE_C));
    }

    private ReplicationGroup normalReplicas(UUID... normalReplicas)
    {
        return new ReplicationGroup(Arrays.asList(normalReplicas));
    }

    private static Token token(long token)
    {
        return new Murmur3Partitioner.LongToken(token);
    }
}