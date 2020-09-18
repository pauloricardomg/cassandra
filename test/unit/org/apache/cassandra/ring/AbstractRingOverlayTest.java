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
import java.util.List;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.utils.UUIDGen;

import static org.assertj.core.api.Assertions.assertThat;

public class AbstractRingOverlayTest
{
    static IPartitioner originalPartitioner;
    static IEndpointSnitch originalSnitch;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        originalPartitioner = DatabaseDescriptor.getPartitioner();
        originalSnitch = DatabaseDescriptor.getEndpointSnitch();
        DatabaseDescriptor.setPartitionerUnsafe(new Murmur3Partitioner());
    }

    @AfterClass
    public static void afterClass()
    {
        /** This may be set during {@link TestCluster} initialization **/
        DatabaseDescriptor.setPartitionerUnsafe(originalPartitioner);
    }

    protected static ReplicaSet normalReplicas(UUID... normalReplicas)
    {
        return new ReplicaSet(Arrays.asList(normalReplicas));
    }

    protected static ReplicaSet replicas(List<UUID> normal, List<UUID> pending)
    {
        return new ReplicaSet(normal, pending);
    }

    protected static Token token(long token)
    {
        return new Murmur3Partitioner.LongToken(token);
    }
}