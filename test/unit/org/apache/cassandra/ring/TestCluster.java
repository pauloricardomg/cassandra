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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.ReplicationFactor;
import org.apache.cassandra.ring.token.TokenState;

public class TestCluster
{
    final MultiDatacenterRing ring;

    private TestCluster(MultiDatacenterRing ring)
    {
        this.ring = ring;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public MultiDatacenterRing getRing()
    {
        return ring;
    }

    public static class Builder
    {
        List<DatacenterBuilder> dcs = new LinkedList<>();

        public TestCluster build()
        {
            Map<String, ReplicationFactor> dcRfs = new HashMap<>();
            RingSnapshot ringSnapshot = new RingSnapshot();

            for (DatacenterBuilder dc : dcs)
            {
                dcRfs.put(dc.dcName, ReplicationFactor.fullOnly(dc.rf));
                for (DatacenterBuilder.NodeBuilder node : dc.nodes)
                {
                    List<TokenState> newStates = Arrays.stream(node.tokens).map(t -> TokenState.normal(token(t), dc.dcName, "r1", node.id))
                                                                           .collect(Collectors.toList());
                    ringSnapshot = ringSnapshot.withAppliedStates(newStates);
                }
            }

            return new TestCluster(new MultiDatacenterRing(ringSnapshot, dcRfs));
        }

        public DatacenterBuilder withDatacenter(String dcName)
        {
            DatacenterBuilder dcBuilder = new DatacenterBuilder(dcName);
            dcs.add(dcBuilder);
            return dcBuilder;
        }

        class DatacenterBuilder
        {
            private final String dcName;
            private final List<NodeBuilder> nodes = new LinkedList<>();

            private int rf = 1;

            DatacenterBuilder(String dcName)
            {
                this.dcName = dcName;
            }

            public DatacenterBuilder withReplicationFactor(int rf)
            {
                this.rf = rf;
                return this;
            }

            public NodeBuilder withNode(UUID id)
            {
                NodeBuilder nodeBuilder = new NodeBuilder(id);
                nodes.add(nodeBuilder);
                return nodeBuilder;
            }

            public TestCluster build()
            {
                return Builder.this.build();
            }

            class NodeBuilder
            {
                private final UUID id;
                private Long[] tokens = new Long[0];

                public NodeBuilder(UUID id)
                {
                    this.id = id;
                }

                public DatacenterBuilder withManualTokens(Long... tokens)
                {
                    this.tokens = tokens;
                    return DatacenterBuilder.this;
                }

                public DatacenterBuilder withRandomTokens()
                {
                    throw new UnsupportedOperationException();
                }
            }
        }
    }

    public static Token token(long token)
    {
        return new Murmur3Partitioner.LongToken(token);
    }
}
