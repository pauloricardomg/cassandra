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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;

/**
 * This class blocks for a quorum of responses _in all datacenters_ (CL.EACH_QUORUM).
 */
public class DatacenterSyncWriteResponseHandler<T> extends AbstractWriteResponseHandler<T>
{
    private static final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();

    private final Map<String, AtomicInteger> responses = new HashMap<String, AtomicInteger>();
    private final AtomicInteger acks = new AtomicInteger(0);

    public DatacenterSyncWriteResponseHandler(Collection<InetAddress> naturalEndpoints,
                                              Collection<InetAddress> pendingEndpoints,
                                              ConsistencyLevel consistencyLevel,
                                              Keyspace keyspace,
                                              Runnable callback,
                                              WriteType writeType,
                                              Predicate<InetAddress> isAlive)
    {
        // Response is been managed by the map so make it 1 for the superclass.
        super(keyspace, naturalEndpoints, pendingEndpoints, consistencyLevel, callback, writeType, isAlive);
        assert consistencyLevel == ConsistencyLevel.EACH_QUORUM;

        NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) keyspace.getReplicationStrategy();

        for (String dc : strategy.getDatacenters())
        {
            int rf = strategy.getReplicationFactor(dc);
            responses.put(dc, new AtomicInteger((rf / 2) + 1));
        }

        // During bootstrap, we have to include the pending endpoints or we may fail the consistency level
        // guarantees (see #833)
        for (InetAddress pending : pendingEndpoints)
        {
            responses.get(snitch.getDatacenter(pending)).incrementAndGet();
        }
    }

    public void response(MessageIn<T> message)
    {
        String dataCenter = message == null
                            ? DatabaseDescriptor.getLocalDataCenter()
                            : snitch.getDatacenter(message.from);

        responses.get(dataCenter).getAndDecrement();
        acks.incrementAndGet();

        for (AtomicInteger i : responses.values())
        {
            if (i.get() > 0)
                return;
        }

        // all the quorum conditions are met
        signal();
    }

    public void assureSufficientLiveNodes() throws UnavailableException
    {
        // It only makes sense for this method to be called prior to sending requests
        // so we can use the expected response counts per-dc to check this property
        // as they won't yet have been decremented by any received responses.
        assert ackCount() == 0;
        Map<String, Integer> endpointsByDc =
            ConsistencyLevel.countPerDCEndpoints(keyspace,
                                                 Iterables.filter(Iterables.concat(naturalEndpoints, pendingEndpoints),
                                                 isAlive));

        for (Map.Entry<String, AtomicInteger> required : responses.entrySet())
        {
            Integer live = endpointsByDc.get(required.getKey());
            if (null == live || live < required.getValue().get())
                throw new UnavailableException(ConsistencyLevel.EACH_QUORUM,
                                               required.getKey(),
                                               live == null ? 0 : live,
                                               required.getValue().get());
        }
    }

    protected int ackCount()
    {
        return acks.get();
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }
}
