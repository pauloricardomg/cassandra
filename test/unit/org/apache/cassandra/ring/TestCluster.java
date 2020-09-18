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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;

public class TestCluster
{
    static final String DEFAULT_DC = "DEFAULT_DC";
    static final String DEFAULT_RACK = "DEFAULT_RACK";

    static Logger logger = LoggerFactory.getLogger(RingSnapshot.class);

    final Map<UUID, NodeInfo> nodesById;
    final Map<InetAddressAndPort, NodeInfo> nodesByAddress;
    final FakeStorageService storageService;
    final VersionedValue.VersionedValueFactory valueFactory;
    final IPAndPortGenerator ipAndPortGenerator;

    public TestCluster(Map<UUID, NodeInfo> nodesById, Map<InetAddressAndPort, NodeInfo> nodesByAddress,
                       IPAndPortGenerator ipAndPortGenerator, FakeStorageService storageService)
    {
        this.nodesById = nodesById;
        this.nodesByAddress = nodesByAddress;
        this.ipAndPortGenerator = ipAndPortGenerator;
        this.storageService = storageService;
        this.valueFactory = new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner());
        initialize();
    }

    public void initialize()
    {
        nodesByAddress.values().forEach(n -> storageService.onChange(n.address, ApplicationState.STATUS_WITH_PORT, valueFactory.normal(n.tokens)));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public RingOverlay getRing()
    {
        return storageService.getRing();
    }

    public void startBootstrap(UUID nodeId, Long... tokens)
    {
        List<Token> nodeTokens = Arrays.stream(tokens).map(t -> token(t)).collect(Collectors.toList());
        NodeInfo node = new NodeInfo(nodeId, DEFAULT_DC, DEFAULT_RACK, nodeTokens, ipAndPortGenerator.generateNext());

        nodesById.put(node.id, node);
        nodesByAddress.put(node.address, node);

        storageService.onChange(node.address, ApplicationState.STATUS_WITH_PORT, valueFactory.bootstrapping(nodeTokens));
    }

    public static class Builder
    {
        IPAndPortGenerator ipAndPortGenerator = new IPAndPortGenerator();
        List<DatacenterBuilder> dcs = new LinkedList<>();

        public TestCluster build(boolean legacy)
        {
            Map<UUID, NodeInfo> nodesById = new HashMap<>();
            Map<InetAddressAndPort, NodeInfo> nodesByAddress = new HashMap<>();
            Map<String, String> dcRfs = new HashMap<>();

            for (DatacenterBuilder dc : dcs)
            {
                dcRfs.put(dc.dcName, dc.rf.toString());

                for (DatacenterBuilder.RackBuilder rack : dc.racks)
                {
                    for (DatacenterBuilder.RackBuilder.NodeBuilder node : rack.nodes)
                    {
                        NodeInfo info = new NodeInfo(node.id, dc.dcName, rack.rackName, Arrays.stream(node.tokens).map(t -> token(t)).collect(Collectors.toList()),
                                                     ipAndPortGenerator.generateNext());

                        logger.info("Creating test node {}", info);

                        nodesById.put(info.id, info);
                        nodesByAddress.put(info.address, info);
                    }
                }
            }

            FakeStorageService storageService = legacy ? new LegacyStorageService(dcRfs, nodesByAddress::get) : new NewStorageService(dcRfs, nodesByAddress::get);
            return new TestCluster(nodesById, nodesByAddress, ipAndPortGenerator, storageService);
        }

        public DatacenterBuilder withDatacenter(String dcName)
        {
            DatacenterBuilder dcBuilder = new DatacenterBuilder(dcName);
            dcs.add(dcBuilder);
            return dcBuilder;
        }

        public DatacenterBuilder withReplicationFactor(int rf)
        {
            return withDatacenter(DEFAULT_DC).withReplicationFactor(rf);
        }

        class DatacenterBuilder
        {
            private final String dcName;
            private final List<RackBuilder> racks = new LinkedList<>();

            private Integer rf = 1;

            DatacenterBuilder(String dcName)
            {
                this.dcName = dcName;
            }

            public DatacenterBuilder withReplicationFactor(int rf)
            {
                this.rf = rf;
                return this;
            }

            public RackBuilder.NodeBuilder withNode(UUID id)
            {
                return withRack(DEFAULT_RACK).withNode(id);
            }

            public TestCluster build(boolean legacy)
            {
                return Builder.this.build(legacy);
            }

            public RackBuilder withRack(String rackName)
            {
                RackBuilder rackBuilder = new RackBuilder(rackName);
                racks.add(rackBuilder);
                return rackBuilder;
            }

            public DatacenterBuilder withDataCenter(String dcName)
            {
                return TestCluster.Builder.this.withDatacenter(dcName);
            }

            class RackBuilder
            {
                final String rackName;
                private final List<NodeBuilder> nodes = new LinkedList<>();

                RackBuilder(String rackName)
                {
                    this.rackName = rackName;
                }

                public NodeBuilder withNode(UUID id)
                {
                    NodeBuilder nodeBuilder = new NodeBuilder(id);
                    nodes.add(nodeBuilder);
                    return nodeBuilder;
                }

                public TestCluster build(Boolean legacy)
                {
                    return Builder.this.build(legacy);
                }

                public DatacenterBuilder and()
                {
                    return DatacenterBuilder.this;
                }

                class NodeBuilder
                {
                    private final UUID id;
                    private Long[] tokens = new Long[0];

                    public NodeBuilder(UUID id)
                    {
                        this.id = id;
                    }

                    public RackBuilder withManualTokens(Long... tokens)
                    {
                        this.tokens = tokens;
                        return RackBuilder.this;
                    }

                    public DatacenterBuilder withRandomTokens()
                    {
                        throw new UnsupportedOperationException();
                    }
                }
            }
        }
    }

    public static Token token(long token)
    {
        return new Murmur3Partitioner.LongToken(token);
    }

    static class IPAndPortGenerator
    {

        private final InetAddress loopbackAddress;
        private int currentPort = 0;

        IPAndPortGenerator()
        {
            try
            {
                this.loopbackAddress = InetAddress.getByName("127.0.0.1");
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }

        public InetAddressAndPort generateNext()
        {
            if (currentPort > 65535)
                throw new RuntimeException("Max number of ports exceeded.");

            return InetAddressAndPort.getByAddressOverrideDefaults(loopbackAddress, currentPort++);
        }
    }
}
