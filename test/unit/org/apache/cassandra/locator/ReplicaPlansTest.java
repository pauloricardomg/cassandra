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

package org.apache.cassandra.locator;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;

import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.cassandra.db.ConsistencyLevel.*;
import static org.apache.cassandra.locator.Replica.fullReplica;
import static org.apache.cassandra.locator.ReplicaUtils.*;
import static org.junit.Assert.fail;

public class ReplicaPlansTest
{

    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    static EnumSet<ConsistencyLevel> writeCls = EnumSet.allOf(ConsistencyLevel.class);
    static
    {
        writeCls.remove(SERIAL);
        writeCls.remove(LOCAL_SERIAL);
        writeCls.remove(NODE_LOCAL);
    }

    static class Snitch extends AbstractNetworkTopologySnitch
    {
        final Set<InetAddressAndPort> dc1;
        Snitch(Set<InetAddressAndPort> dc1)
        {
            this.dc1 = dc1;
        }
        @Override
        public String getRack(InetAddressAndPort endpoint)
        {
            return dc1.contains(endpoint) ? "R1" : "R2";
        }

        @Override
        public String getDatacenter(InetAddressAndPort endpoint)
        {
            return dc1.contains(endpoint) ? "datacenter1" : "datacenter2";
        }
    }

    private static Keyspace ks(Set<InetAddressAndPort> dc1, Map<String, String> replication)
    {
        replication = ImmutableMap.<String, String>builder().putAll(replication).put("class", "NetworkTopologyStrategy").build();
        Keyspace keyspace = Keyspace.mockKS(KeyspaceMetadata.create("blah", KeyspaceParams.create(false, replication)));
        Snitch snitch = new Snitch(dc1);
        DatabaseDescriptor.setEndpointSnitch(snitch);
        keyspace.getReplicationStrategy().snitch = snitch;
        return keyspace;
    }

    private static Replica full(InetAddressAndPort ep) { return fullReplica(ep, R1); }



    @Test
    public void testWriteEachQuorum()
    {
        IEndpointSnitch stash = DatabaseDescriptor.getEndpointSnitch();
        final Token token = tk(1L);
        try
        {
            {
                // all full natural
                Keyspace ks = ks(ImmutableSet.of(EP1, EP2, EP3), ImmutableMap.of("datacenter1", "3", "datacenter2", "3"));
                EndpointsForToken natural = EndpointsForToken.of(token, full(EP1), full(EP2), full(EP3), full(EP4), full(EP5), full(EP6));
                EndpointsForToken pending = EndpointsForToken.empty(token);
                ReplicaPlan.ForTokenWrite plan = ReplicaPlans.forWrite(ks, ConsistencyLevel.EACH_QUORUM, natural, pending, Predicates.alwaysTrue(), ReplicaPlans.writeNormal);
                assertEquals(natural, plan.liveAndDown);
                assertEquals(natural, plan.live);
                assertEquals(natural, plan.contacts());
            }
            {
                // all natural and up, one transient in each DC
                Keyspace ks = ks(ImmutableSet.of(EP1, EP2, EP3), ImmutableMap.of("datacenter1", "3", "datacenter2", "3"));
                EndpointsForToken natural = EndpointsForToken.of(token, full(EP1), full(EP2), trans(EP3), full(EP4), full(EP5), trans(EP6));
                EndpointsForToken pending = EndpointsForToken.empty(token);
                ReplicaPlan.ForTokenWrite plan = ReplicaPlans.forWrite(ks, ConsistencyLevel.EACH_QUORUM, natural, pending, Predicates.alwaysTrue(), ReplicaPlans.writeNormal);
                assertEquals(natural, plan.liveAndDown);
                assertEquals(natural, plan.live);
                EndpointsForToken expectContacts = EndpointsForToken.of(token, full(EP1), full(EP2), full(EP4), full(EP5));
                assertEquals(expectContacts, plan.contacts());
            }
        }
        finally
        {
            DatabaseDescriptor.setEndpointSnitch(stash);
        }

        {
            // test simple

        }
    }

    @Test
    public void verifyWriteAvailabilityChecks() throws Exception
    {
        final Token t = tk(1L);
        // EP1-6 are the initial natural endpoints.
        // EP1-3 are in datacenter1, EP4-6 are in datacenter2
        // EP7-12 will be used as pending endpoints.
        // EP7-9 are in datacenter1, EP10-12 are in datacenter2
        Keyspace ks = ks(ImmutableSet.of(EP1, EP2, EP3, EP7, EP8, EP9), ImmutableMap.of("datacenter1", "3", "datacenter2", "3"));
        EndpointsForToken natural = EndpointsForToken.of(t, full(EP1), full(EP2), full(EP3), full(EP4), full(EP5), full(EP6));

        // All replicas up & no pending endpoints
        verifyConsistencyLevels(ks, natural, pending(t), failureDetector());

        // All replicas up and 1 pending endpoint
        verifyConsistencyLevels(ks, natural, pending(t, full(EP7)), failureDetector());

        // All replicas up and 1 pending endpoint per-dc
        verifyConsistencyLevels(ks, natural, pending(t, full(EP7), full(EP10)), failureDetector());

        // One replica down in the local DC and no pending endpoints
        Predicate<Replica> failureDetector = failureDetector(EP1);
        // ALL should fail, everything else should succeed
        verifyConsistencyLevels(ks, natural, pending(t), failureDetector, ALL);

        // One replica down and 1 pending node in the same DC
        // Expect same as previous, ALL should throw UnavailableException, everything else should succeed
        verifyConsistencyLevels(ks, natural, pending(t, full(EP7)), failureDetector, ALL);

        // Two replicas down in the same DC and none pending
        // Expect ALL, LOCAL_QUORUM & EACH_QUORUM to throw UnavailableException
        failureDetector = failureDetector(EP1, EP2);
        verifyConsistencyLevels(ks, natural, pending(t), failureDetector, ALL, LOCAL_QUORUM, EACH_QUORUM);
        // Add a pending replica in the local DC. Expectation remains the same as the pending endpoint increases
        // the required endpoints for the consistency level. So a local quorum now requires 3 -> (rf=3/2 + 1) + 1
        verifyConsistencyLevels(ks, natural, pending(t, full(EP7)), failureDetector, ALL, LOCAL_QUORUM, EACH_QUORUM);

        // All replicas in the local DC down, without any pending
        // ANY & non-local ONE/TWO/THREE should succeed, everything else (i.e. ALL/LOCAL_QUORUM/EACH_QUORUM/
        // LOCAL_ONE/QUORUM) should throw UnavailableException
        failureDetector = failureDetector(EP1, EP2, EP3);
        verifyConsistencyLevels(ks, natural, pending(t), failureDetector, allWriteCLsExcept(ONE, TWO, THREE, ANY));

        // Add one pending replica, results should be unchanged
        verifyConsistencyLevels(ks, natural, pending(t, full(EP7)), failureDetector, allWriteCLsExcept(ONE, TWO, THREE, ANY));

        // Add 2 more pending replicas, results should still be unchanged
        verifyConsistencyLevels(ks, natural, pending(t, full(EP7), full(EP8), full(EP9)), failureDetector, allWriteCLsExcept(ONE, TWO, THREE, ANY));

        // All replicas in both DCs are down. Everything except ANY should throw UnavailableException
        failureDetector = failureDetector(EP1, EP2, EP3, EP4, EP5, EP6);
        verifyConsistencyLevels(ks, natural, pending(t), failureDetector, allWriteCLsExcept(ANY));

        // Add 3 pending endpoints in each dc, everything except ANY should still fail
        EndpointsForToken allPending = pending(t, full(EP7), full(EP8), full(EP9), full(EP10), full(EP11), full(EP12));
        verifyConsistencyLevels(ks, natural, allPending, failureDetector, allWriteCLsExcept(ANY));
    }

    private static void verifyConsistencyLevels(Keyspace ks,
                                                EndpointsForToken naturalEndpoints,
                                                EndpointsForToken pendingEndpoints,
                                                Predicate<Replica> failureDetector,
                                                ConsistencyLevel...expectedToFail)
    {
        EnumSet<ConsistencyLevel> expectedSuccesses = EnumSet.copyOf(writeCls);
        EnumSet<ConsistencyLevel> expectedFailures = EnumSet.noneOf(ConsistencyLevel.class);
        for (ConsistencyLevel cl : expectedToFail)
        {
            expectedSuccesses.remove(cl);
            expectedFailures.add(cl);
        }

        Set<ConsistencyLevel> unexpectedSuccesses = EnumSet.noneOf(ConsistencyLevel.class);
        Set<ConsistencyLevel> unexpectedFailures = EnumSet.noneOf(ConsistencyLevel.class);

        for (ConsistencyLevel cl : writeCls)
        {
            try
            {
                ReplicaPlans.forWrite(ks, cl, naturalEndpoints, pendingEndpoints, failureDetector, ReplicaPlans.writeAll);
                if (!expectedSuccesses.contains(cl))
                    unexpectedSuccesses.add(cl);
            }
            catch (UnavailableException e)
            {
                if (!expectedFailures.contains(cl))
                    unexpectedFailures.add(cl);
            }
        }

        if (!unexpectedSuccesses.isEmpty() || !unexpectedFailures.isEmpty())
        {
            fail(String.format("Expectations not matched. Unexpected failures : %s, Unexpected successes: %s",
                               unexpectedFailures.stream().map(ConsistencyLevel::name).collect(Collectors.joining(", ")),
                               unexpectedSuccesses.stream().map(ConsistencyLevel::name).collect(Collectors.joining(", "))));
        }
    }

    private static ConsistencyLevel[] allWriteCLsExcept(ConsistencyLevel... exclude)
    {
        EnumSet<ConsistencyLevel> cls = EnumSet.copyOf(writeCls);
        for (ConsistencyLevel ex : exclude)
            cls.remove(ex);
        return cls.toArray(new ConsistencyLevel[cls.size()]);
    }

    private EndpointsForToken pending(Token token, final Replica...replicas)
    {
        return EndpointsForToken.of(token, replicas);
    }

    private Predicate<Replica> failureDetector(final InetAddressAndPort...deadNodes)
    {
        Set<Replica> down = Arrays.stream(deadNodes).map(ReplicaPlansTest::full).collect(Collectors.toSet());
        return inetAddress -> !down.contains(inetAddress);
    }
}
