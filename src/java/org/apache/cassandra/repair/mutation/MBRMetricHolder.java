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

package org.apache.cassandra.repair.mutation;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public class MBRMetricHolder
{
    private final Range<Token> range;
    private final AtomicInteger mismatchedPages = new AtomicInteger();
    private final long startTime = System.currentTimeMillis();
    private final AtomicInteger hashedRows = new AtomicInteger();
    private final AtomicInteger hugePages = new AtomicInteger();
    private final AtomicInteger appliedMutations = new AtomicInteger();
    private final AtomicInteger appliedHugeResponsePage = new AtomicInteger();
    private final ColumnFamilyStore cfs;

    public MBRMetricHolder(ColumnFamilyStore cfs, Range<Token> range)
    {
        this.range = range;
        this.cfs = cfs;
    }

    public void increaseRowsHashed(int hashed)
    {
        cfs.metric.mbrRowsHashed.inc();
        hashedRows.addAndGet(hashed);
    }

    public void mismatchedPage()
    {
        cfs.metric.mbrMismatchedPageHashes.inc();
        mismatchedPages.incrementAndGet();
    }

    public void hugePage()
    {
        cfs.metric.mbrHugePage.inc();
        hugePages.incrementAndGet();
    }

    public void appliedMutation()
    {
        cfs.metric.mbrTotalRows.inc();
        appliedMutations.incrementAndGet();
    }

    public void appliedHugeResponseMutation()
    {
        cfs.metric.mbrAppliedHugeResponseMutations.inc();
        appliedHugeResponsePage.incrementAndGet();
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("repaired page for %s.%s - ", cfs.keyspace.getName(), cfs.getTableName()));
        sb.append(String.format("time spent: %dms, ", System.currentTimeMillis() - startTime));
        sb.append(String.format("hashed rows: %d, ", hashedRows.get()));
        sb.append(String.format("mismatched pages: %d, ", mismatchedPages.get()));
        sb.append(String.format("huge pages: %d, ", hugePages.get()));
        sb.append(String.format("applied mutations: %d, ", appliedMutations.get()));
        sb.append(String.format("applied huge response mutations: %d, ", appliedHugeResponsePage.get()));
        return sb.toString();
    }
}
