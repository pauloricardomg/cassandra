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

package org.apache.cassandra.db.view;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.service.AtomicBatchWriter;

public class ViewWriteHandler
{
    public static final ViewWriteHandler EMPTY = new ViewWriteHandler(null, null);

    private Optional<AtomicLong> baseComplete;
    private final Optional<AtomicBatchWriter.AtomicBatchWriteHandler> batchlogWriteHandlerFuture;

    public ViewWriteHandler(AtomicLong baseComplete, AtomicBatchWriter.AtomicBatchWriteHandler batchlogTableWriteFuture)
    {
        this.baseComplete = Optional.ofNullable(baseComplete);
        this.batchlogWriteHandlerFuture = Optional.ofNullable(batchlogTableWriteFuture);
    }

    public void baseComplete()
    {
        this.baseComplete.ifPresent(l -> l.set(System.currentTimeMillis()));
        this.batchlogWriteHandlerFuture.ifPresent(f -> f.ackMutation());
    }

    public void waitForLocalWrites()
    {
        this.batchlogWriteHandlerFuture.ifPresent(f -> f.waitForLocalWrites());
    }
}
