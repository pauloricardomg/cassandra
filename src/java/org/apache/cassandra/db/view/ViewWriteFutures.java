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

import org.apache.cassandra.service.AtomicBatchWriter;

public class ViewWriteFutures
{
    public static final ViewWriteFutures EMPTY = new ViewWriteFutures(null);

    private final Optional<AtomicBatchWriter.AtomicBatchWriteHandler> batchlogWriteHandlerFuture;

    public ViewWriteFutures(AtomicBatchWriter.AtomicBatchWriteHandler batchlogTableWriteFuture)
    {
        this.batchlogWriteHandlerFuture = Optional.ofNullable(batchlogTableWriteFuture);
    }

    public void baseComplete()
    {
        this.batchlogWriteHandlerFuture.ifPresent(f -> f.ackMutation());
    }

    public void waitForLocalWrites()
    {
        this.batchlogWriteHandlerFuture.ifPresent(f -> f.waitForLocalWrites());
    }
}
