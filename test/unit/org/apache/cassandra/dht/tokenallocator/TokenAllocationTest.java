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
package org.apache.cassandra.dht.tokenallocator;

import java.util.Collection;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tools.Util;

import com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;


public class TokenAllocationTest
{
    @Before
    public void setup()
    {
        Util.initDatabaseDescriptor();
    }

    /**
     * Cycle through a matrix of valid ranges.
     */
    @Test
    public void testTokenGenerations()
    {
        for (int numTokens = 1; numTokens <= 16 ; ++numTokens)
        {
            for (int rf = 1; rf <=5; ++rf)
            {
                int nodes = 32;
                for (int racks = rf; racks <= Math.min(10, nodes); ++racks)
                {
                    int[] nodeToRack = makeIdToRackMap(nodes, racks);
                    for (IPartitioner partitioner : new IPartitioner[] { Murmur3Partitioner.instance, RandomPartitioner.instance })
                    {
                        TokenAllocator<Integer> allocator = TokenAllocation.createTokenGenerator(rf, nodeToRack, partitioner);
                        Collection<Token> allTokens = Lists.newArrayList();
                        for (int node = 0; node < nodes; ++node)
                        {
                            Collection<Token> allocatedTokens = allocator.addUnit(node, numTokens);
                            Assertions.assertThat(allocatedTokens).hasSize(numTokens);
                            Assertions.assertThat(allTokens).doesNotContainAnyElementsOf(allocatedTokens);
                            allTokens.addAll(allocatedTokens);
                        }
                    }
                }
            }
        }
    }

    private int[] makeIdToRackMap(int nodes, int racks)
    {
        assert nodes > 0;
        assert racks > 0;
        // Distribute nodes among the racks in round-robin fashion in the order the user is supposed to start them.
        int[] idToRack;
        idToRack = new int[nodes];
        int rack = 0;
        for (int node = 0; node < nodes; node++)
        {
            idToRack[node] = rack;
            if (++rack == racks)
                rack = 0;
        }
        return idToRack;
    }

    @Test(expected = AssertionError.class)
    public void testTokenGenerator_more_rf_than_racks()
    {
        TokenAllocation.createTokenGenerator(3, new int[]{1, 2}, Murmur3Partitioner.instance).addUnit(0, 16);
    }

    @Test
    public void testTokenGenerator_single_rack_or_single_rf()
    {
        // Simple cases, single rack or single replication.
        TokenAllocation.createTokenGenerator(1, null, Murmur3Partitioner.instance).addUnit(0, 16);
        TokenAllocation.createTokenGenerator(1, new int[]{}, Murmur3Partitioner.instance).addUnit(0, 16);
        TokenAllocation.createTokenGenerator(1, new int[]{1}, Murmur3Partitioner.instance).addUnit(0, 16);
        TokenAllocation.createTokenGenerator(1, new int[]{1, 1}, Murmur3Partitioner.instance).addUnit(0, 16);
        TokenAllocation.createTokenGenerator(2, null, Murmur3Partitioner.instance).addUnit(0, 16);
        TokenAllocation.createTokenGenerator(2, new int[]{}, Murmur3Partitioner.instance).addUnit(0, 16);
        TokenAllocation.createTokenGenerator(2, new int[]{1}, Murmur3Partitioner.instance).addUnit(0, 16);
        TokenAllocation.createTokenGenerator(2, new int[]{1, 1}, Murmur3Partitioner.instance).addUnit(0, 16);
    }

    @Test
    public void testTokenGenerator_rf1_repeat_ok()
    {
        TokenAllocator<Integer> allocator = TokenAllocation.createTokenGenerator(1, null, Murmur3Partitioner.instance);
        allocator.addUnit(0, 16);
        allocator.addUnit(0, 16);
        allocator.addUnit(1, 16);
        allocator.addUnit(2, 16);
        allocator = TokenAllocation.createTokenGenerator(1, new int[]{}, Murmur3Partitioner.instance);
        allocator.addUnit(0, 16);
        allocator.addUnit(0, 16);
        allocator.addUnit(1, 16);
        allocator.addUnit(2, 16);
        allocator = TokenAllocation.createTokenGenerator(1, new int[]{1}, Murmur3Partitioner.instance);
        allocator.addUnit(0, 16);
        allocator.addUnit(0, 16);
        allocator.addUnit(1, 16);
        allocator.addUnit(2, 16);
        allocator = TokenAllocation.createTokenGenerator(1, new int[]{1,2,3}, Murmur3Partitioner.instance);
        allocator.addUnit(0, 16);
        allocator.addUnit(0, 16);
        allocator.addUnit(1, 16);
        allocator.addUnit(2, 16);
    }

    @Test(expected = AssertionError.class)
    public void testTokenGenerator_rf1_once_only_0()
    {
        TokenAllocator<Integer> allocator = TokenAllocation.createTokenGenerator(1, new int[]{1,2,3}, Murmur3Partitioner.instance);
        allocator.addUnit(0, 16);
        allocator.addUnit(1, 16);
        allocator.addUnit(1, 16);
    }

    @Test(expected = AssertionError.class)
    public void testTokenGenerator_rf1_once_only_1()
    {
        TokenAllocator<Integer> allocator = TokenAllocation.createTokenGenerator(1, new int[]{1,2,3}, Murmur3Partitioner.instance);
        allocator.addUnit(0, 16);
        allocator.addUnit(1, 16);
        allocator.addUnit(2, 16);
        allocator.addUnit(0, 16);
    }

    @Test
    public void testTokenGenerator_rf3_repeat_ok()
    {
        TokenAllocator<Integer> allocator = TokenAllocation.createTokenGenerator(3, null, Murmur3Partitioner.instance);
        allocator.addUnit(0, 16);
        allocator.addUnit(1, 16);
        allocator.addUnit(2, 16);
        allocator = TokenAllocation.createTokenGenerator(3, new int[]{}, Murmur3Partitioner.instance);
        allocator.addUnit(0, 16);
        allocator.addUnit(1, 16);
        allocator.addUnit(2, 16);
        allocator = TokenAllocation.createTokenGenerator(3, new int[]{1}, Murmur3Partitioner.instance);
        allocator.addUnit(0, 16);
        allocator.addUnit(1, 16);
        allocator.addUnit(2, 16);
        allocator = TokenAllocation.createTokenGenerator(3, new int[]{1,2,3}, Murmur3Partitioner.instance);
        allocator.addUnit(0, 16);
        allocator.addUnit(0, 16);
        allocator.addUnit(1, 16);
        allocator.addUnit(1, 16);
        allocator.addUnit(2, 16);
        allocator.addUnit(2, 16);
    }

    @Test(expected = AssertionError.class)
    public void testTokenGenerator_rf3_once_only_0()
    {
        TokenAllocator<Integer> allocator = TokenAllocation.createTokenGenerator(3, null, Murmur3Partitioner.instance);
        allocator.addUnit(0, 16);
        allocator.addUnit(0, 16);
    }

    @Test(expected = AssertionError.class)
    public void testTokenGenerator_rf3_once_only_1()
    {
        TokenAllocator<Integer> allocator = TokenAllocation.createTokenGenerator(3, new int[]{1}, Murmur3Partitioner.instance);
        allocator.addUnit(0, 16);
        allocator.addUnit(0, 16);
    }

    @Test(expected = AssertionError.class)
    public void testTokenGenerator_rf3_once_only_2()
    {
        TokenAllocator<Integer> allocator = TokenAllocation.createTokenGenerator(3, new int[]{1, 1, 1}, Murmur3Partitioner.instance);
        allocator.addUnit(0, 16);
        allocator.addUnit(0, 16);
    }

    @Test(expected = AssertionError.class)
    public void testTokenGenerator_rf3_once_only_3()
    {
        TokenAllocator<Integer> allocator = TokenAllocation.createTokenGenerator(3, new int[]{1, 2, 3, 1}, Murmur3Partitioner.instance);
        allocator.addUnit(0, 16);
        allocator.addUnit(1, 16);
        allocator.addUnit(2, 16);
        allocator.addUnit(3, 16);
        allocator.addUnit(3, 16);
    }
}
