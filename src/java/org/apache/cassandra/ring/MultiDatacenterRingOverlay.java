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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.ReplicationFactor;
import org.apache.cassandra.ring.token.VirtualNode;
import org.assertj.core.util.VisibleForTesting;

public class MultiDatacenterRingOverlay implements RingOverlay
{
    static LinkedHashSet<Object> EMPTY = new LinkedHashSet<>();

    @VisibleForTesting
    protected final RingSnapshot ringSnapshot;
    @VisibleForTesting
    protected final Map<String, ReplicationFactor> dcRfs;
    private final ReplicationFactor aggregateRf;

    final Map<String, Set<String>> dcRacks = new HashMap<>();
    final Map<String, Integer> maxRepeatsPerRack = new HashMap<>();

    public MultiDatacenterRingOverlay(RingSnapshot ringSnapshot, Map<String, ReplicationFactor> dcRfs)
    {
        this.ringSnapshot = ringSnapshot;
        this.dcRfs = dcRfs;

        /**
         * Initialize {@link this#dcRacks}
         */
        Iterator<VirtualNode> ring = ringSnapshot.iterator();
        while (ring.hasNext())
        {
            VirtualNode vnode = ring.next();
            dcRacks.compute(vnode.dc, (dc, racks) -> racks == null ? new HashSet<>() : racks).add(vnode.rack);
        }

        /**
         * Initialize {@link this#maxRepeatsPerRack}
         */
        int totalRF = 0;
        for (Map.Entry<String, ReplicationFactor> dcRFs : dcRfs.entrySet())
        {
            String dcName = dcRFs.getKey();
            int dcRf = dcRFs.getValue().fullReplicas;
            totalRF += dcRf;
            maxRepeatsPerRack.put(dcName, Math.max(0, dcRf - dcRacks.get(dcName).size()));
        }

        aggregateRf = ReplicationFactor.fullOnly(totalRF);
    }

    class ReplicaSetBuilder
    {
        LinkedHashSet<UUID> replicas = new LinkedHashSet<UUID>();
        LinkedHashSet<UUID> pendingReplicas = ringSnapshot.hasPendingNodes() ? new LinkedHashSet<>() : null;

        Map<String, Integer> replicasByDc = new HashMap<>();
        Map<String, Map<String, Integer>> replicasByRack = new HashMap<>();

        boolean satisfiesRF = aggregateRf.fullReplicas == 0;

        public boolean maybeAdd(VirtualNode vnode)
        {
            if (satisfiesRF)
                return false;

            if (vnode.isAdding())
            {
                pendingReplicas.add(vnode.owner);
                return true;
            }

            // Do not allow multiple VNodes per host
            if (replicas.contains(vnode.owner))
                return false;

            // Maximum RF replicas per DC
            Integer dcReplicas = replicasByDc.getOrDefault(vnode.dc, 0);
            if (dcReplicas == dcRfs.getOrDefault(vnode.dc, ReplicationFactor.ZERO).fullReplicas)
                return false;

            // Maximum (1 + MAX_REPEATS_PER_RACK) replicas per rack
            Integer rackReplicas = replicasByRack.getOrDefault(vnode.dc, Collections.emptyMap()).getOrDefault(vnode.rack, 0);
            if (rackReplicas > maxRepeatsPerRack.getOrDefault(vnode.dc, 0))
                return false;

            replicasByDc.compute(vnode.dc, (k, v) -> v == null ? 1 : v + 1);
            replicasByRack.compute(vnode.dc, (k, v) -> v == null ? new HashMap<>() : v).compute(vnode.rack, (k, v) -> v == null ? 1 : v + 1);
            replicas.add(vnode.owner);

            satisfiesRF = replicasByDc.values().stream().mapToInt(v -> v).sum() == aggregateRf.fullReplicas;
            return true;
        }

        public boolean satisfiesReplicationFactor()
        {
            return satisfiesRF;
        }

        public ReplicaSet build()
        {
            return new ReplicaSet(new ArrayList<>(replicas), pendingReplicas == null ? Collections.EMPTY_LIST : new ArrayList<>(pendingReplicas));
        }
    }

    @Override
    public ReplicaSet getWriteReplicas(Token token)
    {
        ReplicaSetBuilder replicas = new ReplicaSetBuilder();

        RingIterator ring = ringSnapshot.iterator();
        ring.advanceToToken(token);

        while (ring.hasNext() && !replicas.satisfiesReplicationFactor())
        {
            VirtualNode vnode = ring.next();
            replicas.maybeAdd(vnode);
        }

        return replicas.build();
    }

}
