/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

import static org.apache.cassandra.Util.token;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.dht.BigIntegerToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.StorageServiceAccessor;

@RunWith(OrderedJUnit4ClassRunner.class)
public class TokenMetadataTest extends SchemaLoader
{
    public final static String ONE = "1";
    public final static String SIX = "6";

    static TokenMetadata tmd;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        tmd = StorageService.instance.getTokenMetadata();
        tmd.updateNormalToken(token(ONE), InetAddress.getByName("127.0.0.1"));
        tmd.updateNormalToken(token(SIX), InetAddress.getByName("127.0.0.6"));
    }

    private static void testRingIterator(ArrayList<Token> ring, String start, boolean includeMin, String... expected)
    {
        ArrayList<Token> actual = new ArrayList<>();
        Iterators.addAll(actual, TokenMetadata.ringIterator(ring, token(start), includeMin));
        assertEquals(actual.toString(), expected.length, actual.size());
        for (int i = 0; i < expected.length; i++)
            assertEquals("Mismatch at index " + i + ": " + actual, token(expected[i]), actual.get(i));
    }

    @Test
    public void testRingIterator()
    {
        ArrayList<Token> ring = tmd.sortedTokens();
        testRingIterator(ring, "2", false, "6", "1");
        testRingIterator(ring, "7", false, "1", "6");
        testRingIterator(ring, "0", false, "1", "6");
        testRingIterator(ring, "", false, "1", "6");
    }

    @Test
    public void testRingIteratorIncludeMin()
    {
        ArrayList<Token> ring = tmd.sortedTokens();
        testRingIterator(ring, "2", true, "6", "", "1");
        testRingIterator(ring, "7", true, "", "1", "6");
        testRingIterator(ring, "0", true, "1", "6", "");
        testRingIterator(ring, "", true, "1", "6", "");
    }

    @Test
    public void testRingIteratorEmptyRing()
    {
        testRingIterator(new ArrayList<Token>(), "2", false);
    }

    @Test
    public void testTopologyUpdate_RackConsolidation() throws UnknownHostException
    {
        final InetAddress first = InetAddress.getByName("127.0.0.1");
        final InetAddress second = InetAddress.getByName("127.0.0.6");
        final String DATA_CENTER = "datacenter1";
        final String RACK1 = "rack1";
        final String RACK2 = "rack2";

        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddress endpoint)
            {
                return endpoint.equals(first) ? RACK1 : RACK2;
            }

            @Override
            public String getDatacenter(InetAddress endpoint)
            {
                return DATA_CENTER;
            }

            @Override
            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
            {
                return 0;
            }
        });

        tmd.updateNormalToken(token(ONE), first);
        tmd.updateNormalToken(token(SIX), second);

        TokenMetadata tokenMetadata = tmd.cloneOnlyTokenMap();
        assertNotNull(tokenMetadata);

        TokenMetadata.Topology topology = tokenMetadata.getTopology();
        assertNotNull(topology);

        Multimap<String, InetAddress> allEndpoints = topology.getDatacenterEndpoints();
        assertNotNull(allEndpoints);
        assertTrue(allEndpoints.size() == 2);
        assertTrue(allEndpoints.containsKey(DATA_CENTER));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(first));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(second));

        Map<String, Multimap<String, InetAddress>> racks = topology.getDatacenterRacks();
        assertNotNull(racks);
        assertTrue(racks.size() == 1);
        assertTrue(racks.containsKey(DATA_CENTER));
        assertTrue(racks.get(DATA_CENTER).size() == 2);
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK1));
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK2));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(first));
        assertTrue(racks.get(DATA_CENTER).get(RACK2).contains(second));

        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddress endpoint)
            {
                return RACK1;
            }

            @Override
            public String getDatacenter(InetAddress endpoint)
            {
                return DATA_CENTER;
            }

            @Override
            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
            {
                return 0;
            }
        });

        tokenMetadata.updateTopology(first);
        tokenMetadata.updateTopology(second);

        allEndpoints = topology.getDatacenterEndpoints();
        assertNotNull(allEndpoints);
        assertTrue(allEndpoints.size() == 2);
        assertTrue(allEndpoints.containsKey(DATA_CENTER));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(first));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(second));

        racks = topology.getDatacenterRacks();
        assertNotNull(racks);
        assertTrue(racks.size() == 1);
        assertTrue(racks.containsKey(DATA_CENTER));
        assertTrue(racks.get(DATA_CENTER).size() == 2);
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK1));
        assertFalse(racks.get(DATA_CENTER).containsKey(RACK2));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(first));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(second));
    }

    @Test
    public void testTopologyUpdate_RackExpansion() throws UnknownHostException
    {
        final InetAddress first = InetAddress.getByName("127.0.0.1");
        final InetAddress second = InetAddress.getByName("127.0.0.6");
        final String DATA_CENTER = "datacenter1";
        final String RACK1 = "rack1";
        final String RACK2 = "rack2";

        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddress endpoint)
            {
                return RACK1;
            }

            @Override
            public String getDatacenter(InetAddress endpoint)
            {
                return DATA_CENTER;
            }

            @Override
            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
            {
                return 0;
            }
        });

        tmd.updateNormalToken(token(ONE), first);
        tmd.updateNormalToken(token(SIX), second);

        TokenMetadata tokenMetadata = tmd.cloneOnlyTokenMap();
        assertNotNull(tokenMetadata);

        TokenMetadata.Topology topology = tokenMetadata.getTopology();
        assertNotNull(topology);

        Multimap<String, InetAddress> allEndpoints = topology.getDatacenterEndpoints();
        assertNotNull(allEndpoints);
        assertTrue(allEndpoints.size() == 2);
        assertTrue(allEndpoints.containsKey(DATA_CENTER));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(first));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(second));

        Map<String, Multimap<String, InetAddress>> racks = topology.getDatacenterRacks();
        assertNotNull(racks);
        assertTrue(racks.size() == 1);
        assertTrue(racks.containsKey(DATA_CENTER));
        assertTrue(racks.get(DATA_CENTER).size() == 2);
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK1));
        assertFalse(racks.get(DATA_CENTER).containsKey(RACK2));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(first));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(second));

        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddress endpoint)
            {
                return endpoint.equals(first) ? RACK1 : RACK2;
            }

            @Override
            public String getDatacenter(InetAddress endpoint)
            {
                return DATA_CENTER;
            }

            @Override
            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
            {
                return 0;
            }
        });

        tokenMetadata.updateTopology();

        allEndpoints = topology.getDatacenterEndpoints();
        assertNotNull(allEndpoints);
        assertTrue(allEndpoints.size() == 2);
        assertTrue(allEndpoints.containsKey(DATA_CENTER));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(first));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(second));

        racks = topology.getDatacenterRacks();
        assertNotNull(racks);
        assertTrue(racks.size() == 1);
        assertTrue(racks.containsKey(DATA_CENTER));
        assertTrue(racks.get(DATA_CENTER).size() == 2);
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK1));
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK2));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(first));
        assertTrue(racks.get(DATA_CENTER).get(RACK2).contains(second));
    }

    @Test
    public void testIsMemberOrPending() throws UnknownHostException
    {
        // the token difference will be RING_SIZE * 2.
        final int RING_SIZE = 10;
        TokenMetadata tmd = new TokenMetadata();
        TokenMetadata oldTmd = StorageServiceAccessor.setTokenMetadata(tmd);

        Token[] endpointTokens = new Token[RING_SIZE];
        Token[] keyTokens = new Token[RING_SIZE];

        for (int i = 0; i < RING_SIZE; i++)
        {
            endpointTokens[i] = new BigIntegerToken(String.valueOf(RING_SIZE * 2 * i));
            keyTokens[i] = new BigIntegerToken(String.valueOf(RING_SIZE * 2 * i + RING_SIZE));
        }

        List<InetAddress> hosts = new ArrayList<InetAddress>();
        for (int i = 0; i < endpointTokens.length; i++)
        {
            InetAddress ep = InetAddress.getByName("127.0.0." + String.valueOf(i + 1));
            tmd.updateNormalToken(endpointTokens[i], ep);
            hosts.add(ep);
        }

        // bootstrap at the end of the ring
        Token bsToken = new BigIntegerToken(String.valueOf(210));
        InetAddress bootstrapEndpoint = InetAddress.getByName("127.0.0.11");
        tmd.addBootstrapToken(bsToken, bootstrapEndpoint);

        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            assertFalse(tmd.isMemberOrPending(bootstrapEndpoint));
        }

        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            AbstractReplicationStrategy strategy = getStrategy(keyspaceName, tmd);
            PendingRangeCalculatorService.calculatePendingRanges(strategy, keyspaceName);
        }

        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            assertTrue(tmd.isMemberOrPending(bootstrapEndpoint));
        }

        tmd.removeEndpoint(bootstrapEndpoint);

        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            assertFalse(tmd.isMemberOrPending(bootstrapEndpoint));
        }

        StorageServiceAccessor.setTokenMetadata(oldTmd);
    }

    private AbstractReplicationStrategy getStrategy(String keyspaceName, TokenMetadata tmd)
    {
        KSMetaData ksmd = Schema.instance.getKSMetaData(keyspaceName);
        return AbstractReplicationStrategy.createReplicationStrategy(
                                                                    keyspaceName,
                                                                    ksmd.strategyClass,
                                                                    tmd,
                                                                    new SimpleSnitch(),
                                                                    ksmd.strategyOptions);
    }
}
