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

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.locator.ReplicationFactor;
import org.apache.cassandra.ring.token.TokenState;

public class ReplicatedRing
{
    private final RingSnapshot ringSnapshot;
    private final Map<String, ReplicationFactor> dcRfs;

    public ReplicatedRing(RingSnapshot ringSnapshot, Map<String, ReplicationFactor> dcRfs)
    {
        this.ringSnapshot = ringSnapshot;
        this.dcRfs = dcRfs;
    }

    public ReplicaLayout.ForTokenWrite getReplicasForTokenWrite(Token token)
    {
        LinkedHashSet<UUID> normalReplicas = new LinkedHashSet<>();
        LinkedHashSet<UUID> pendingReplicas = new LinkedHashSet<>();

        for (Map.Entry<String, ReplicationFactor> dcRf : dcRfs.entrySet())
        {
            ReplicationFactor rf = dcRf.getValue();

            int acceptableRackRepeats = rf.allReplicas - ringSnapshot.getRackCount(dcRf.getKey());
            Set<String> seenRacks = new HashSet<>();

            RingIterator it = ringSnapshot.filterByDc(dcRf.getKey());

            it.advanceToToken(token);

            TokenState currentToken = it.next();

            while (normalReplicas.size() < rf.allReplicas)
            {
                while (currentToken.isAdding())
                {
                    pendingReplicas.add(currentToken.owner);
                    currentToken = it.next();
                }

                if (!normalReplicas.contains(currentToken.owner) &&
                        (acceptableRackRepeats > 0 || !seenRacks.contains(currentToken.rack)))
                {
                    if (currentToken.isRemoving())
                    {
                        TokenState nextFromSameRack = it.peekNextFromRack(currentToken.rack);
                        pendingReplicas.add(nextFromSameRack.owner);
                    }

                    acceptableRackRepeats--;
                    seenRacks.add(currentToken.rack);
                    normalReplicas.add(currentToken.owner);
                }

                currentToken = it.next();
            }
        }

        return null;
    }

}
