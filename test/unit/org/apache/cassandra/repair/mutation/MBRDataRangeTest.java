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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertTrue;


public class MBRDataRangeTest extends CQLTester
{
    @Test
    public void wrappingTest() throws Throwable
    {
        createTable("create table %s (id int, id2 int, something text, PRIMARY KEY (id, id2))");

        for (int j = 0; j < 5; j++)
        {
            for (int i = 0; i < 10; i++)
            {
                execute("insert into %s (id, id2, something) values (?, ?, ?)", j, i, i + ":" + j);
            }
        }
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        Set<Pair<Integer, Integer>> expectedPks = new HashSet<>();
        expectedPks.add(Pair.create(0,6));
        expectedPks.add(Pair.create(0,7));
        expectedPks.add(Pair.create(0,8));
        expectedPks.add(Pair.create(0,9));
        for (int i = 0; i < 10; i++)
            expectedPks.add(Pair.create(2, i));
        expectedPks.add(Pair.create(4,0));
        expectedPks.add(Pair.create(4,1));
        expectedPks.add(Pair.create(4,2));
        expectedPks.add(Pair.create(4,3));

        AbstractBounds<PartitionPosition> range = AbstractBounds.bounds(cfs.decorateKey(ByteBufferUtil.bytes(0)), true, cfs.decorateKey(ByteBufferUtil.bytes(4)), true);
        ClusteringBound start = ClusteringBound.exclusiveStartOf(ByteBufferUtil.bytes(5));
        ClusteringBound end = ClusteringBound.inclusiveEndOf(ByteBufferUtil.bytes(3));

        DataRange dr3 = new MBRService.MBRDataRange(range, cfs.metadata.comparator, start, end);
        PartitionRangeReadCommand prrc = new PartitionRangeReadCommand(cfs.metadata,
                                                                       FBUtilities.nowInSeconds(),
                                                                       ColumnFilter.all(cfs.metadata),
                                                                       RowFilter.NONE,
                                                                       DataLimits.NONE,
                                                                       dr3,
                                                                       Optional.empty());

        executeReadCommand(prrc, expectedPks);
        assertTrue(expectedPks.isEmpty());
    }
    @Test
    public void singlePartitionTest() throws Throwable
    {
        createTable("create table %s (id int, id2 int, something text, PRIMARY KEY (id, id2))");

        for (int i = 0; i < 100; i++)
        {
            execute("insert into %s (id, id2, something) values (?, ?, ?)", 1, i, i + ":" + 1);
        }
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        Set<Pair<Integer, Integer>> expectedPks = new HashSet<>();
        for (int i = 0; i < 10; i++)
            expectedPks.add(Pair.create(1, i + 30));

        AbstractBounds<PartitionPosition> range = AbstractBounds.bounds(cfs.decorateKey(ByteBufferUtil.bytes(1)), true, cfs.decorateKey(ByteBufferUtil.bytes(1)), true);
        ClusteringBound start = ClusteringBound.exclusiveStartOf(ByteBufferUtil.bytes(29));
        ClusteringBound end = ClusteringBound.inclusiveEndOf(ByteBufferUtil.bytes(39));

        DataRange dr3 = new MBRService.MBRDataRange(range, cfs.metadata.comparator, start, end);
        PartitionRangeReadCommand prrc = new PartitionRangeReadCommand(cfs.metadata,
                                                                       FBUtilities.nowInSeconds(),
                                                                       ColumnFilter.all(cfs.metadata),
                                                                       RowFilter.NONE,
                                                                       DataLimits.NONE,
                                                                       dr3,
                                                                       Optional.empty());

        executeReadCommand(prrc, expectedPks);
        assertTrue(expectedPks.isEmpty());
    }

    private void executeReadCommand(PartitionRangeReadCommand prrc, Set<Pair<Integer, Integer>> expectedPks)
    {
        try (ReadExecutionController ec = prrc.executionController();
             PartitionIterator pi = prrc.executeInternal(ec))
        {
            while(pi.hasNext())
            {
                try (RowIterator ri = pi.next())
                {

                    int partKey = ByteBufferUtil.toInt(ri.partitionKey().getKey());
                    while (ri.hasNext())
                    {
                        Row r = ri.next();

                        int clustering = ByteBufferUtil.toInt(r.clustering().getRawValues()[0]);
                        expectedPks.remove(Pair.create(partKey, clustering));
                    }
                }
            }
        }
    }
}
