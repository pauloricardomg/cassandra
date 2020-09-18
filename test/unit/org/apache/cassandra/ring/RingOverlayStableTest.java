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
import java.util.Collection;
import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.UUIDGen;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class RingOverlayStableTest extends AbstractRingOverlayTest
{
    static final String DC_1 = "DC_1";
    static final String DC_2 = "DC_2";

    static final String RACK_R1 = "R1";
    static final String RACK_R2 = "R2";
    static final String RACK_R3 = "R3";
    static final String RACK_R4 = "R3";

    static final UUID NODE_A = UUIDGen.getTimeUUID();
    static final UUID NODE_B = UUIDGen.getTimeUUID();
    static final UUID NODE_C = UUIDGen.getTimeUUID();
    static final UUID NODE_D = UUIDGen.getTimeUUID();
    static final UUID NODE_E = UUIDGen.getTimeUUID();

    @Parameterized.Parameters(name = "legacy={0}")
    public static Collection<Object[]> input()
    {
        return Arrays.asList(new Object[][]{{Boolean.FALSE}, {Boolean.TRUE}});
    }

    private final Boolean legacy;

    public RingOverlayStableTest(Boolean legacy)
    {
        this.legacy = legacy;
    }

    /**
     * Method: {@link RingOverlay#getWriteReplicas(Token)}
     * - Data center count: 1
     * - Rack count: 1
     * - Nodes: 5
     * - RF: 3
     */
    @Test
    public void testGetWriteReplicas_1dc_1rack_5nodes_rf3()
    {
        // Node's Tokens
        // | A: [0, 150, 650]    |  C: [200, 350, 850]  |  E: [400, 550, 1050]
        // | B: [100, 250, 750]  |  D: [300, 450, 950]  |
        TestCluster cluster = TestCluster.builder()
                                         .withDatacenter(DC_1).withReplicationFactor(3)
                                         .withNode(NODE_A).withManualTokens(0L, 150L, 650L)
                                         .withNode(NODE_B).withManualTokens(100L, 250L, 750L)
                                         .withNode(NODE_C).withManualTokens(200L, 350L, 850L)
                                         .withNode(NODE_D).withManualTokens(300L, 450L, 950L)
                                         .withNode(NODE_E).withManualTokens(400L, 550L, 1050L)
                                         .build(legacy);

        // RING LAYOUT
        // <--[0:A]--[100:B]--[150:A]--[200:C]--[250:B]--[300:D]--[350:C]-->
        // <--[400:E]--[450:D]--[550:E]--[650:A]--[750:B]--[850:C]--[950:D]--[1050:E]-->
        RingOverlay ring = cluster.getRing();

        // Token 10: [100:B]--[150:A]--[200:C]
        assertThat(ring.getWriteReplicas(token(10L))).isEqualTo(normalReplicas(NODE_B, NODE_A, NODE_C));

        // Token 110: [150:A]--[200:C]--[250:B]
        assertThat(ring.getWriteReplicas(token(110L))).isEqualTo(normalReplicas(NODE_A, NODE_C, NODE_B));

        // Token 160: [200:C]--[250:B]--[300:D]
        assertThat(ring.getWriteReplicas(token(160L))).isEqualTo(normalReplicas(NODE_C, NODE_B, NODE_D));

        // Token 210: [250:B]--[300:D]--[350:C]
        assertThat(ring.getWriteReplicas(token(210L))).isEqualTo(normalReplicas(NODE_B, NODE_D, NODE_C));

        // Token 260: [300:D]--[350:C]--[400:E]
        assertThat(ring.getWriteReplicas(token(260L))).isEqualTo(normalReplicas(NODE_D, NODE_C, NODE_E));

        // Token 310: [350:C]--[400:E]--[450:D]
        assertThat(ring.getWriteReplicas(token(310L))).isEqualTo(normalReplicas(NODE_C, NODE_E, NODE_D));

        // Token 360: [400:E]--[450:D]--[550:E]*--[650:A]
        // * vnode [550:E] is skipped because node E already replicates this range
        assertThat(ring.getWriteReplicas(token(360L))).isEqualTo(normalReplicas(NODE_E, NODE_D, NODE_A));

        // Token 410: [450:D]--[550:E]--[650:A]
        assertThat(ring.getWriteReplicas(token(410L))).isEqualTo(normalReplicas(NODE_D, NODE_E, NODE_A));

        // Token 460: [550:E]--[650:A]--[750:B]
        assertThat(ring.getWriteReplicas(token(460L))).isEqualTo(normalReplicas(NODE_E, NODE_A, NODE_B));

        // Token 560: [650:A]--[750:B]--[850:C]
        assertThat(ring.getWriteReplicas(token(560L))).isEqualTo(normalReplicas(NODE_A, NODE_B, NODE_C));

        // Token 660: [750:B]--[850:C]--[950:D]
        assertThat(ring.getWriteReplicas(token(660L))).isEqualTo(normalReplicas(NODE_B, NODE_C, NODE_D));

        // Token 760: [850:C]--[950:D]--[1050:E]
        assertThat(ring.getWriteReplicas(token(760L))).isEqualTo(normalReplicas(NODE_C, NODE_D, NODE_E));

        // Token 860: [950:D]--[1050:E]--[0:A]
        assertThat(ring.getWriteReplicas(token(860L))).isEqualTo(normalReplicas(NODE_D, NODE_E, NODE_A));

        // Token 960: [1050:E]--[0:A]--[100:B]
        assertThat(ring.getWriteReplicas(token(960L))).isEqualTo(normalReplicas(NODE_E, NODE_A, NODE_B));

        // Token 1060: [0:A]--[100:B]--[150:A]*--[200:C]
        // * vnode [150:A] is skipped because node A already replicates this range
        assertThat(ring.getWriteReplicas(token(1060L))).isEqualTo(normalReplicas(NODE_A, NODE_B, NODE_C));
    }

    /**
     * Method: {@link RingOverlay#getWriteReplicas(Token)}
     * - Data center count: 1
     * - Rack count: 2
     * - Nodes: 5
     * - RF: 3
     */
    @Test
    public void testGetWriteReplicas_1dc_2racks_5nodes_rf3()
    {
        // Node's Tokens
        // R1: | A: [0, 150, 650]    |  B: [100, 250, 750]  |  C: [200, 350, 850]
        // R2: | D: [300, 450, 950]  |  E: [400, 550, 1050]
        TestCluster cluster = TestCluster.builder()
                                         .withDatacenter(DC_1).withReplicationFactor(3)
                                            .withRack(RACK_R1)
                                                .withNode(NODE_A).withManualTokens(0L, 150L, 650L)
                                                .withNode(NODE_B).withManualTokens(100L, 250L, 750L)
                                                .withNode(NODE_C).withManualTokens(200L, 350L, 850L).and()
                                            .withRack(RACK_R2)
                                                .withNode(NODE_D).withManualTokens(300L, 450L, 950L)
                                                .withNode(NODE_E).withManualTokens(400L, 550L, 1050L).build(legacy);

        // RING LAYOUT
        // <--[0:R1:A]--[100:R1:B]--[150:R1:A]--[200:R1:C]--[250:R1:B]--[300:R2:D]--[350:R1:C]-->
        // <--[400:R2:E]--[450:R2:D]--[550:R2:E]--[650:R1:A]--[750:R1:B]--[850:R1:C]--[950:R2:D]--[1050:R2:E]-->
        RingOverlay ring = cluster.getRing();

        // Token 10: [100:R1:B]--[150:R1:A]--[200:R1:C]*--[250:R1:B]*--[300:R2:D]
        // * vnodes [200:R1:C], [250:R1:B] are skipped because rack R1 already has 2 replicas (maxRepeatsPerRack)
        assertThat(ring.getWriteReplicas(token(10L))).isEqualTo(normalReplicas(NODE_B, NODE_A, NODE_D));

        // Token 110: [150:R1:A]--[200:R1:C]--[250:R1:B]*--[250:R1:B]--[300:R2:D]
        // * vnode [250:R1:B] is skipped because rack R1 already has 2 replicas (maxRepeatsPerRack)
        assertThat(ring.getWriteReplicas(token(110L))).isEqualTo(normalReplicas(NODE_A, NODE_C, NODE_D));

        // Token 160: [200:R1:C]--[250:R1:B]--[300:R2:D]
        assertThat(ring.getWriteReplicas(token(160L))).isEqualTo(normalReplicas(NODE_C, NODE_B, NODE_D));

        // Token 210: [250:R1:B]--[300:R2:D]--[350:R1:C]
        assertThat(ring.getWriteReplicas(token(210L))).isEqualTo(normalReplicas(NODE_B, NODE_D, NODE_C));

        // Token 260: [300:R2:D]--[350:R1:C]--[400:R2:E]
        assertThat(ring.getWriteReplicas(token(260L))).isEqualTo(normalReplicas(NODE_D, NODE_C, NODE_E));

        // Token 310: [350:R1:C]--[400:R2:E]--[450:R2:D]
        assertThat(ring.getWriteReplicas(token(310L))).isEqualTo(normalReplicas(NODE_C, NODE_E, NODE_D));

        // Token 360: [400:R2:E]--[450:R2:D]--[550:R2:E]*--[650:R1:A]
        // * vnode [550:R2:E] is skipped because node E already replicates this range
        assertThat(ring.getWriteReplicas(token(360L))).isEqualTo(normalReplicas(NODE_E, NODE_D, NODE_A));

        // Token 410: [450:R2:D]--[550:R2:E]--[650:R1:A]
        assertThat(ring.getWriteReplicas(token(410L))).isEqualTo(normalReplicas(NODE_D, NODE_E, NODE_A));

        // Token 460: [550:R2:E]--[650:R1:A]--[750:R1:B]
        assertThat(ring.getWriteReplicas(token(460L))).isEqualTo(normalReplicas(NODE_E, NODE_A, NODE_B));

        // Token 560: [650:R1:A]--[750:R1:B]--[850:R1:C]*--[950:R2:D]
        // * vnode [850:R1:C] is skipped because rack R1 already has 2 replicas (maxRepeatsPerRack)
        assertThat(ring.getWriteReplicas(token(560L))).isEqualTo(normalReplicas(NODE_A, NODE_B, NODE_D));

        // Token 660: [750:R1:B]--[850:R1:C]--[950:R2:D]
        assertThat(ring.getWriteReplicas(token(660L))).isEqualTo(normalReplicas(NODE_B, NODE_C, NODE_D));

        // Token 760: [850:R1:C]--[950:R2:D]--[1050:R2:E]
        assertThat(ring.getWriteReplicas(token(760L))).isEqualTo(normalReplicas(NODE_C, NODE_D, NODE_E));

        // Token 860: [950:R2:D]--[1050:R2:E]--[0:R1:A]
        assertThat(ring.getWriteReplicas(token(860L))).isEqualTo(normalReplicas(NODE_D, NODE_E, NODE_A));

        // Token 960: [1050:R2:E]--[0:R1:A]--[100:R1:B]
        assertThat(ring.getWriteReplicas(token(960L))).isEqualTo(normalReplicas(NODE_E, NODE_A, NODE_B));

        // Token 1060: [0:R1:A]--[100:R1:B]--[150:R1:A]*--[200:R1:C]*--[250:R1:B]*--[300:R2:D]
        // * vnodes [200:R1:C], [250:R1:B] are skipped because rack R1 already has 2 replicas (maxRepeatsPerRack)
        // * vnode [150:R1:A] is skipped because node A already replicates this range
        assertThat(ring.getWriteReplicas(token(1060L))).isEqualTo(normalReplicas(NODE_A, NODE_B, NODE_D));
    }

    /**
     * Method: {@link RingOverlay#getWriteReplicas(Token)}
     * - Data center count: 1
     * - Rack count: 3
     * - Nodes: 5
     * - RF: 3
     */
    @Test
    public void testGetWriteReplicas_1dc_3rack_5nodes_rf3()
    {
        // Node's Tokens
        // R1: A: [0, 150, 650]    |   D: [300, 450, 950]
        // R2: B: [100, 250, 750]  |   E: [400, 550, 1050]
        // R3: C: [200, 350, 850] (with only one node on R3, node C should repliate all ranges)
        TestCluster cluster = TestCluster.builder()
                                         .withDatacenter(DC_1).withReplicationFactor(3)
                                            .withRack(RACK_R1).withNode(NODE_A).withManualTokens(0L, 150L, 650L)
                                                              .withNode(NODE_D).withManualTokens(300L, 450L, 950L).and()
                                            .withRack(RACK_R2).withNode(NODE_B).withManualTokens(100L, 250L, 750L)
                                                              .withNode(NODE_E).withManualTokens(400L, 550L, 1050L).and()
                                            .withRack(RACK_R3).withNode(NODE_C).withManualTokens(200L, 350L, 850L).build(legacy);

        // RING LAYOUT
        // <--[0:R1:A]--[100:R2:B]--[150:R1:A]--[200:R3:C]--[250:R2:B]--[300:R1:D]--[350:R3:C]-->
        // <--[400:R2:E]--[450:R1:D]--[550:R2:E]--[650:R1:A]--[750:R2:B]--[850:R3:C]--[950:R1:D]--[1050:R2:E]-->
        RingOverlay ring = cluster.getRing();

        // Token 10: [100:R2:B]--[150:R1:A]--[200:R3:C]
        assertThat(ring.getWriteReplicas(token(10L))).isEqualTo(normalReplicas(NODE_B, NODE_A, NODE_C));

        // Token 110: [150:R1:A]--[200:R3:C]--[250:R2:B]
        assertThat(ring.getWriteReplicas(token(110L))).isEqualTo(normalReplicas(NODE_A, NODE_C, NODE_B));

        // Token 160: [200:R3:C]--[250:R2:B]--[300:R1:D]
        assertThat(ring.getWriteReplicas(token(160L))).isEqualTo(normalReplicas(NODE_C, NODE_B, NODE_D));

        // Token 210: [250:R2:B]--[300:R1:D]--[350:R3:C]
        assertThat(ring.getWriteReplicas(token(210L))).isEqualTo(normalReplicas(NODE_B, NODE_D, NODE_C));

        // Token 260: [300:R1:D]--[350:R3:C]--[400:R2:E]
        assertThat(ring.getWriteReplicas(token(260L))).isEqualTo(normalReplicas(NODE_D, NODE_C, NODE_E));

        // Token 310: [350:R3:C]--[400:R2:E]--[450:R1:D]
        assertThat(ring.getWriteReplicas(token(310L))).isEqualTo(normalReplicas(NODE_C, NODE_E, NODE_D));

        // Token 360: [400:R2:E]--[450:R1:D]--[550:R2:E]*--[650:R1:A]*--[750:R2:B]*--[850:R3:C]
        // * vnodes [550:R2:E], [650:R1:A], [750:R2:B] are skipped because racks R1 and R2 already replicate this range
        assertThat(ring.getWriteReplicas(token(360L))).isEqualTo(normalReplicas(NODE_E, NODE_D, NODE_C));

        // Token 410: [450:R1:D]--[550:R2:E]--[650:R1:A]*--[750:R2:B]*--[850:R3:C]
        // * vnodes [650:R1:A], [750:R2:B] are skipped because racks R1 and R2 already replicate this range
        assertThat(ring.getWriteReplicas(token(410L))).isEqualTo(normalReplicas(NODE_D, NODE_E, NODE_C));

        // Token 460: [550:R2:E]--[650:R1:A]--[750:R2:B]*--[850:R3:C]
        // * vnode [750:R2:B] is skipped because racks R1 and R2 already replicate this range
        assertThat(ring.getWriteReplicas(token(460L))).isEqualTo(normalReplicas(NODE_E, NODE_A, NODE_C));

        // Token 560: [750:R2:B]--[850:R3:C]--[950:R1:D]
        assertThat(ring.getWriteReplicas(token(560L))).isEqualTo(normalReplicas(NODE_A, NODE_B, NODE_C));

        // Token 660: [750:R2:B]--[850:R3:C]--[950:R1:D]
        assertThat(ring.getWriteReplicas(token(660L))).isEqualTo(normalReplicas(NODE_B, NODE_C, NODE_D));

        // Token 760: [850:R3:C]--[950:R1:D]--[1050:R2:E]
        assertThat(ring.getWriteReplicas(token(760L))).isEqualTo(normalReplicas(NODE_C, NODE_D, NODE_E));

        // Token 860: [950:R1:D]--[1050:R2:E]--[0:R1:A]*--[100:R2:B]*--[150:R1:A]*--[200:R3:C]
        // * vnodes [0:R1:A], [100:R2:B], [150:R1:A] are skipped because racks R1 and R2 already replicate this range
        assertThat(ring.getWriteReplicas(token(860L))).isEqualTo(normalReplicas(NODE_D, NODE_E, NODE_C));

        // Token 960: [1050:R2:E]--[0:R1:A]--[100:R2:B]*--[150:R1:A]*--[200:R3:C]
        // * vnodes [100:R2:B], [150:R1:A] are skipped because racks R1 and R2 already replicate this range
        assertThat(ring.getWriteReplicas(token(960L))).isEqualTo(normalReplicas(NODE_E, NODE_A, NODE_C));

        // Token 1060: [0:R1:A]--[100:R2:B]--[150:R1:A]*--[200:R3:C]
        // * vnode [150:R1:A]* is skipped because node A already replicates this range
        assertThat(ring.getWriteReplicas(token(1060L))).isEqualTo(normalReplicas(NODE_A, NODE_B, NODE_C));
    }

    /**
     * Method: {@link RingOverlay#getWriteReplicas(Token)}
     * - Data center count: 2
     * - Rack count: 1
     * - Nodes: 5
     * - RF: {2, 2}
     */
    @Test
    public void testGetWriteReplicas_2dc_1rack_5nodes_rf4()
    {
        // Node's Tokens
        // DC1: A: [0, 150, 650]    |   C: [200, 350, 850]    |   E: [400, 550, 1050]
        // DC2: B: [100, 250, 750]  |   D: [300, 450, 950]
        TestCluster cluster = TestCluster.builder()
                                         .withDatacenter(DC_1).withReplicationFactor(2)
                                            .withNode(NODE_A).withManualTokens(0L, 150L, 650L)
                                            .withNode(NODE_C).withManualTokens(200L, 350L, 850L)
                                            .withNode(NODE_E).withManualTokens(400L, 550L, 1050L).and()
                                         .withDataCenter(DC_2).withReplicationFactor(2)
                                            .withNode(NODE_D).withManualTokens(300L, 450L, 950L).and()
                                            .withNode(NODE_B).withManualTokens(100L, 250L, 750L).build(legacy);

        // RING LAYOUT
        // <--[0:DC1:A]--[100:DC2:B]--[150:DC1:A]--[200:DC1:C]--[250:DC2:B]--[300:DC2:D]--[350:DC1:C]-->
        // <--[400:DC1:E]--[450:DC2:R4:D]--[550:DC1:E]--[650:DC1:A]--[750:DC2:B]--[850:DC1:C]--[950:DC2:D]--[1050:DC1:E]-->
        RingOverlay ring = cluster.getRing();

        // Token 10: [100:DC2:B]--[150:DC1:A]--[200:DC1:C]--[250:DC2:B]*--[300:DC2:D]
        // * vnode [250:DC2:B] is skipped because node B already replicates this range
        assertThat(ring.getWriteReplicas(token(10L))).isEqualTo(normalReplicas(NODE_B, NODE_A, NODE_C, NODE_D));

        // Token 110: [150:DC1:A]--[200:DC1:C]--[250:DC2:B]--[300:DC2:D]
        assertThat(ring.getWriteReplicas(token(110L))).isEqualTo(normalReplicas(NODE_A, NODE_C, NODE_B, NODE_D));

        // Token 160: [200:DC1:C]--[250:DC2:B]--[300:DC2:D]--[350:DC1:C]*--[400:DC1:E]
        // * vnode [350:DC1:C] is skipped because node C already replicates this range
        assertThat(ring.getWriteReplicas(token(160L))).isEqualTo(normalReplicas(NODE_C, NODE_B, NODE_D, NODE_E));

        // Token 210: [250:DC2:B]--[300:DC2:D]--[350:DC1:C]--[400:DC1:E]
        assertThat(ring.getWriteReplicas(token(210L))).isEqualTo(normalReplicas(NODE_B, NODE_D, NODE_C, NODE_E));

        // Token 260: [300:DC2:D]--[350:DC1:C]--[400:DC1:E]--[450:DC2:D]*--[550:DC1:E]*--[650:DC1:A]*--[750:DC2:B]
        // * vnodes [450:DC2:D], [550:DC1:E] are skipped because node D and E already replicate this range
        // * vnode [650:DC1:A] is skipped because RF=2 nodes from DC1 already replicate this range
        assertThat(ring.getWriteReplicas(token(260L))).isEqualTo(normalReplicas(NODE_D, NODE_C, NODE_E, NODE_B));

        // Token 310: [350:DC1:C]--[400:DC1:E]--[450:DC2:D]-[550:DC1:E]*--[650:DC1:A]*--[750:DC2:B]
        // * vnodes [550:DC1:E], [650:DC1:A] are skipped because RF=2 nodes from DC1 already replicate this range
        assertThat(ring.getWriteReplicas(token(310L))).isEqualTo(normalReplicas(NODE_C, NODE_E, NODE_D, NODE_B));

        // Token 360: [400:DC1:E]--[450:DC2:D]--[550:DC1:E]*--[650:DC1:A]--[750:DC2:B]
        // * vnode [550:DC1:E] is skipped because node E already replicates this range
        assertThat(ring.getWriteReplicas(token(360L))).isEqualTo(normalReplicas(NODE_E, NODE_D, NODE_A, NODE_B));

        // Token 410: [450:DC2:D]--[550:DC1:E]--[650:DC1:A]--[750:DC2:B]
        assertThat(ring.getWriteReplicas(token(410L))).isEqualTo(normalReplicas(NODE_D, NODE_E, NODE_A, NODE_B));

        // Token 460: [550:DC1:E]--[650:DC1:A]--[750:DC2:B]--[850:DC1:C]*--[950:DC2:D]
        // * vnode [850:DC1:C] is skipped because RF=2 nodes from DC1 already replicate this range
        assertThat(ring.getWriteReplicas(token(460L))).isEqualTo(normalReplicas(NODE_E, NODE_A, NODE_B, NODE_D));

        // Token 560: [650:DC1:A]--[750:DC2:B]--[850:DC1:C]--[950:DC2:D]
        assertThat(ring.getWriteReplicas(token(560L))).isEqualTo(normalReplicas(NODE_A, NODE_B, NODE_C, NODE_D));

        // Token 660: [750:DC2:B]--[850:DC1:C]--[950:DC2:D]--[1050:DC1:E]
        assertThat(ring.getWriteReplicas(token(660L))).isEqualTo(normalReplicas(NODE_B, NODE_C, NODE_D, NODE_E));

        // Token 760: [850:DC1:C]--[950:DC2:D]--[1050:DC1:E]--[0:DC1:A]*--[100:DC2:B]
        // * vnode [0:DC1:A] is skipped because RF=2 nodes from DC1 already replicate this range
        assertThat(ring.getWriteReplicas(token(760L))).isEqualTo(normalReplicas(NODE_C, NODE_D, NODE_E, NODE_B));

        // Token 860: [950:DC2:D]--[1050:DC1:E]--[0:DC1:A]--[100:DC2:B]
        assertThat(ring.getWriteReplicas(token(860L))).isEqualTo(normalReplicas(NODE_D, NODE_E, NODE_A, NODE_B));

        // Token 960: [1050:DC1:E]--[0:DC1:A]--[100:DC2:B]--[150:DC1:A]*--[200:DC1:C]*--[250:DC2:B]*--[300:DC2:D]
        // * vnodes [150:DC1:A], [250:DC2:B] are skipped because nodes A and B already replicates this range
        // * vnode [200:DC1:C] is skipped because RF=2 nodes from DC1 already replicate this range
        assertThat(ring.getWriteReplicas(token(960L))).isEqualTo(normalReplicas(NODE_E, NODE_A, NODE_B, NODE_D));

        // Token 1060: [0:DC1:A]--[100:DC2:B]--[150:DC1:A]*--[200:DC1:C]--[250:DC2:B]*--[300:DC2:D]
        // * vnode [150:DC1:A] is skipped because node A already replicates this range
        assertThat(ring.getWriteReplicas(token(1060L))).isEqualTo(normalReplicas(NODE_A, NODE_B, NODE_C, NODE_D));
    }

    /**
     * Method: {@link RingOverlay#getWriteReplicas(Token)}
     * - Data center count: 2
     * - Rack count: 2
     * - Nodes: 5
     * - RF: {2, 2}
     */
    @Test
    public void testGetWriteReplicas_2dc_2racks_5nodes_rf4()
    {
        // Node's Tokens
        // DC1:
        // - R1: A: [0, 150, 650]   | E: [400, 550, 1050]
        // - R2: C: [200, 350, 850]
        // DC2:
        // - R1: B: [100, 250, 750]
        // - R2: D: [300, 450, 950]
        TestCluster cluster = TestCluster.builder()
                                         .withDatacenter(DC_1).withReplicationFactor(2)
                                            .withRack(RACK_R1)
                                                .withNode(NODE_A).withManualTokens(0L, 150L, 650L)
                                                .withNode(NODE_E).withManualTokens(400L, 550L, 1050L).and()
                                            .withRack(RACK_R2)
                                                .withNode(NODE_C).withManualTokens(200L, 350L, 850L).and()
                                         .withDataCenter(DC_2).withReplicationFactor(2)
                                            .withRack(RACK_R3).withNode(NODE_D).withManualTokens(300L, 450L, 950L).and()
                                            .withRack(RACK_R4).withNode(NODE_B).withManualTokens(100L, 250L, 750L).build(legacy);

        // RING LAYOUT
        // <--[0:DC1:R1:A]--[100:DC2:R3:B]--[150:DC1:R1:A]--[200:DC1:R2:C]--[250:DC2:R3:B]--[300:DC2:R4:D]--[350:DC1:R2:C]-->
        // <--[400:DC1:R1:E]--[450:DC2:R4:D]--[550:DC1:R1:E]--[650:DC1:R1:A]--[750:DC2:R3:B]--[850:DC1:R2:C]--[950:DC2:R4:D]--[1050:DC1:R1:E]-->
        RingOverlay ring = cluster.getRing();

        // Token 10: [100:DC2:R3:B]--[150:DC1:R1:A]--[200:DC1:R2:C]--[250:DC2:R3:B]*--[300:DC2:R4:D]
        // * vnode [250:DC2:R3:B] is skipped because node B already replicates this range
        assertThat(ring.getWriteReplicas(token(10L))).isEqualTo(normalReplicas(NODE_B, NODE_A, NODE_C, NODE_D));

        // Token 110: [150:DC1:R1:A]--[200:DC1:R2:C]--[250:DC2:R3:B]--[300:DC2:R4:D]
        assertThat(ring.getWriteReplicas(token(110L))).isEqualTo(normalReplicas(NODE_A, NODE_C, NODE_B, NODE_D));

        // Token 160: [200:DC1:R2:C]--[250:DC2:R3:B]--[300:DC2:R4:D]--[350:DC1:R2:C]*--[400:DC1:R1:E]
        // * vnode [350:DC1:R2:C] is skipped because node C already replicates this range
        assertThat(ring.getWriteReplicas(token(160L))).isEqualTo(normalReplicas(NODE_C, NODE_B, NODE_D, NODE_E));

        // Token 210: [250:DC2:R3:B]--[300:DC2:R4:D]--[350:DC1:R2:C]--[400:DC1:R1:E]
        assertThat(ring.getWriteReplicas(token(210L))).isEqualTo(normalReplicas(NODE_B, NODE_D, NODE_C, NODE_E));

        // Token 260: [300:DC2:R4:D]--[350:DC1:R2:C]--[400:DC1:R1:E]--[450:DC2:R4:D]*--[550:DC1:R1:E]*--[650:DC1:R1:A]*--[750:DC2:R3:B]
        // * vnodes [450:DC2:R4:D], [550:DC1:R1:E] are skipped because node D and E already replicate this range
        // * vnode [650:DC1:R1:A] is skipped because RF=2 nodes from DC1 already replicate this range
        assertThat(ring.getWriteReplicas(token(260L))).isEqualTo(normalReplicas(NODE_D, NODE_C, NODE_E, NODE_B));

        // Token 310: [350:DC1:R2:C]--[400:DC1:R1:E]--[450:DC2:R4:D]-[550:DC1:R1:E]*--[650:DC1:R1:A]*--[750:DC2:R3:B]
        // * vnodes [550:DC1:R1:E], [650:DC1:R1:A] are skipped because RF=2 nodes from DC1 already replicate this range
        assertThat(ring.getWriteReplicas(token(310L))).isEqualTo(normalReplicas(NODE_C, NODE_E, NODE_D, NODE_B));

        // Token 360: [400:DC1:R1:E]--[450:DC2:R4:D]--[550:DC1:R1:E]*--[650:DC1:R1:A]*--[750:DC2:R3:B]--[850:DC1:R2:C]
        // * vnode [550:DC1:R1:E] is skipped because node E already replicates this range
        // * vnode [650:DC1:R1:A] is skipped because rack R1 already replicates this range
        assertThat(ring.getWriteReplicas(token(360L))).isEqualTo(normalReplicas(NODE_E, NODE_D, NODE_B, NODE_C));

        // Token 410: [450:DC2:R4:D]--[550:DC1:R1:E]--[650:DC1:R1:A]*--[750:DC2:R3:B]--[850:DC1:R2:C]
        // * vnode [650:DC1:R1:A] is skipped because rack R1 already replicates this range
        assertThat(ring.getWriteReplicas(token(410L))).isEqualTo(normalReplicas(NODE_D, NODE_E, NODE_B, NODE_C));

        // Token 460: [550:DC1:R1:E]--[650:DC1:R1:A]*--[750:DC2:R3:B]--[850:DC1:R2:C]--[950:DC2:R4:D]
        // * vnode [850:DC1:R2:C] is skipped because RF=2 nodes from DC1 already replicate this range
        assertThat(ring.getWriteReplicas(token(460L))).isEqualTo(normalReplicas(NODE_E, NODE_B, NODE_C, NODE_D));

        // Token 560: [650:DC1:R1:A]--[750:DC2:R3:B]--[850:DC1:R2:C]--[950:DC2:R4:D]
        assertThat(ring.getWriteReplicas(token(560L))).isEqualTo(normalReplicas(NODE_A, NODE_B, NODE_C, NODE_D));

        // Token 660: [750:DC2:R3:B]--[850:DC1:R2:C]--[950:DC2:R4:D]--[1050:DC1:R1:E]
        assertThat(ring.getWriteReplicas(token(660L))).isEqualTo(normalReplicas(NODE_B, NODE_C, NODE_D, NODE_E));

        // Token 760: [850:DC1:R2:C]--[950:DC2:R4:D]--[1050:DC1:R1:E]--[0:DC1:R1:A]*--[100:DC2:R3:B]
        // * vnode [0:DC1:R1:A] is skipped because RF=2 nodes from DC1 already replicate this range
        assertThat(ring.getWriteReplicas(token(760L))).isEqualTo(normalReplicas(NODE_C, NODE_D, NODE_E, NODE_B));

        // Token 860: [950:DC2:R4:D]--[1050:DC1:R1:E]--[0:DC1:R1:A]*--[100:DC2:R3:B]--[150:DC1:R1:A]*--[200:DC1:R2:C]
        // * vnodes [0:DC1:R1:A], [150:DC1:R1:A] are skipped because rack R1 already replicates this range
        assertThat(ring.getWriteReplicas(token(860L))).isEqualTo(normalReplicas(NODE_D, NODE_E, NODE_B, NODE_C));

        // Token 960: [1050:DC1:R1:E]--[0:DC1:R1:A]*--[100:DC2:R3:B]--[150:DC1:R1:A]*--[200:DC1:R2:C]--[250:DC2:R3:B]*--[300:DC2:R4:D]
        // * vnodes [0:DC1:R1:A], [150:DC1:R1:A] are skipped because rack R1 already replicates this range
        // * vnode [250:DC2:R3:B] is skipped because node A already replicates this range
        assertThat(ring.getWriteReplicas(token(960L))).isEqualTo(normalReplicas(NODE_E, NODE_B, NODE_C, NODE_D));

        // Token 1060: [0:DC1:R1:A]--[100:DC2:R3:B]--[150:DC1:R1:A]*--[200:DC1:R2:C]--[250:DC2:R3:B]*--[300:DC2:R4:D]
        // * vnode [150:DC1:R1:A] is skipped because node A already replicates this range
        // * vnode [250:DC2:R3:B] is skipped because node B already replicates this range
        assertThat(ring.getWriteReplicas(token(1060L))).isEqualTo(normalReplicas(NODE_A, NODE_B, NODE_C, NODE_D));
    }
}