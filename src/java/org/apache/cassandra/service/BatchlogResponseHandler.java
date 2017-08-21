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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.MessageIn;

public class BatchlogResponseHandler<T> extends AbstractWriteResponseHandler<T>
{
    private final AtomicBatchWriter.BatchlogCleanup cleanup;
    AbstractWriteResponseHandler<T> wrapped;
    protected volatile int responsesBeforeAck;
    private static final AtomicIntegerFieldUpdater<BatchlogResponseHandler> requiredBeforeFinishUpdater = AtomicIntegerFieldUpdater.newUpdater(BatchlogResponseHandler.class, "responsesBeforeAck");

    public BatchlogResponseHandler(Keyspace keyspace,
                                   Collection<InetAddress> naturalEndpoints,
                                   Collection<InetAddress> pendingEndpoints,
                                   ConsistencyLevel writeCL,
                                   ConsistencyLevel batchCL,
                                   WriteType writeType,
                                   long queryStartNanoTime,
                                   AtomicBatchWriter.BatchlogCleanup batchlogCleanup)
    {
        super(keyspace, naturalEndpoints, pendingEndpoints, writeCL, null, writeType, queryStartNanoTime);
        this.wrapped = keyspace.getReplicationStrategy().getWriteResponseHandler(naturalEndpoints, pendingEndpoints, writeCL, null, writeType, queryStartNanoTime);
        this.responsesBeforeAck = batchCL.blockFor(keyspace);
        this.cleanup = batchlogCleanup;
    }

    protected int ackCount()
    {
        return wrapped.ackCount();
    }

    public void response(MessageIn<T> msg)
    {
        wrapped.response(msg);
        if (requiredBeforeFinishUpdater.decrementAndGet(this) == 0)
            cleanup.ackMutation();
    }

    public boolean isLatencyForSnitch()
    {
        return wrapped.isLatencyForSnitch();
    }

    public void onFailure(InetAddress from, RequestFailureReason failureReason)
    {
        wrapped.onFailure(from, failureReason);
    }

    public void assureSufficientLiveNodes()
    {
        wrapped.assureSufficientLiveNodes();
    }

    public void get() throws WriteTimeoutException, WriteFailureException
    {
        wrapped.get();
    }

    protected int totalBlockFor()
    {
        return wrapped.totalBlockFor();
    }

    protected int totalEndpoints()
    {
        return wrapped.totalEndpoints();
    }

    protected boolean waitingFor(InetAddress from)
    {
        return wrapped.waitingFor(from);
    }

    protected void signal()
    {
        wrapped.signal();
    }
}