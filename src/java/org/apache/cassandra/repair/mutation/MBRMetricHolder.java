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

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

// todo: update global metrics
public class MBRMetricHolder
{
    private final Range<Token> range;
    private final AtomicInteger mismatchedPages = new AtomicInteger();
    private final long startTime = System.currentTimeMillis();
    private final AtomicInteger hashedRows = new AtomicInteger();
    private final AtomicInteger hugePages = new AtomicInteger();
    private final AtomicInteger appliedMutations = new AtomicInteger();
    private final AtomicInteger appliedHugeResponsePage = new AtomicInteger();

    public MBRMetricHolder(Range<Token> range)
    {
        this.range = range;
    }

    public void increaseRowsHashed(int hashed)
    {
        hashedRows.addAndGet(hashed);
    }

    public void mismatchedPage()
    {
        mismatchedPages.incrementAndGet();
    }

    public void hugePage()
    {
        hugePages.incrementAndGet();
    }

    public void appliedMutation()
    {
        appliedMutations.incrementAndGet();
    }

    public void appliedHugeResponseMutation()
    {
        appliedHugeResponsePage.incrementAndGet();
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("time spent: ").append(System.currentTimeMillis() - startTime).append("ms").append(", ");
        sb.append("hashed rows: ").append(hashedRows).append(", ");
        sb.append("mismatched pages: ").append(mismatchedPages).append(", ");
        sb.append("huge pages: ").append(hugePages).append(", ");
        sb.append("applied mutations: ").append(appliedMutations).append(", ");
        sb.append("applied huge responses: ").append(appliedHugeResponsePage);
        return sb.toString();
    }
}
