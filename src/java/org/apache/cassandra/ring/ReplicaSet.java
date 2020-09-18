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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

public class ReplicaSet
{
    // TODO: include RingSnapshot and/or Token
    final List<UUID> replicas;
    final List<UUID> pendingReplicas;

    public ReplicaSet(List<UUID> replicas, List<UUID> pendingReplicas)
    {
        this.replicas = replicas;
        this.pendingReplicas = pendingReplicas;
    }

    public ReplicaSet(List<UUID> replicas)
    {
        this(replicas, Collections.EMPTY_LIST);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    static class Builder
    {
        List<UUID> normalReplicas;

        public Builder withNaturalReplicas(UUID... replicas)
        {
            normalReplicas = Arrays.asList(replicas);
            return this;
        }

        public ReplicaSet build()
        {
            return new ReplicaSet(normalReplicas);
        }
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicaSet that = (ReplicaSet) o;
        return Objects.equals(replicas, that.replicas) &&
               Objects.equals(pendingReplicas, that.pendingReplicas);
    }

    public int hashCode()
    {
        return Objects.hash(replicas, pendingReplicas);
    }

    public String toString()
    {
        return String.format("[%s%s]", replicasAsString(replicas),
                                          pendingReplicas.isEmpty() ? "" : String.format("|%s", replicasAsString(pendingReplicas)));
    }

    private static String replicasAsString(List<UUID> replicas)
    {
        return replicas.stream().map(r -> r.toString().substring(0, 8)).collect(Collectors.joining(", "));
    }
}
