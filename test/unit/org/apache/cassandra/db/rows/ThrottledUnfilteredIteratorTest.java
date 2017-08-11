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

package org.apache.cassandra.db.rows;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ThrottledUnfilteredIteratorTest extends CQLTester
{
    static final TableMetadata metadata;
    static final ColumnMetadata v1Metadata;
    static final ColumnMetadata v2Metadata;

    static
    {
        metadata = TableMetadata.builder("", "")
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck1", Int32Type.instance)
                                .addClusteringColumn("ck2", Int32Type.instance)
                                .addRegularColumn("v1", Int32Type.instance)
                                .addRegularColumn("v2", Int32Type.instance)
                                .build();
        v1Metadata = metadata.regularAndStaticColumns().columns(false).getSimple(0);
        v2Metadata = metadata.regularAndStaticColumns().columns(false).getSimple(1);
    }

    @Test
    public void complexThrottleWithTombstoneTest() throws Throwable
    {
        // create cell tombstone, range tombstone, partition deletion
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v1 int, v2 int, PRIMARY KEY (pk, ck1, ck2))");

        for (int ck1 = 1; ck1 <= 150; ck1++)
            for (int ck2 = 1; ck2 <= 150; ck2++)
            {
                int timestamp = ck1, v1 = ck1, v2 = ck2;
                execute("INSERT INTO %s(pk,ck1,ck2,v1,v2) VALUES(1,?,?,?,?) using timestamp "
                        + timestamp, ck1, ck2, v1, v2);
            }

        for (int ck1 = 1; ck1 <= 100; ck1++)
            for (int ck2 = 1; ck2 <= 100; ck2++)
            {
                if (ck1 % 2 == 0 || ck1 % 3 == 0) // range tombstone
                    execute("DELETE FROM %s USING TIMESTAMP 170 WHERE pk=1 AND ck1=?", ck1);
                else if (ck1 == ck2) // row tombstone
                    execute("DELETE FROM %s USING TIMESTAMP 180 WHERE pk=1 AND ck1=? AND ck2=?", ck1, ck2);
                else if (ck1 == ck2 - 1) // cell tombstone
                    execute("DELETE v2 FROM %s USING TIMESTAMP 190 WHERE pk=1 AND ck1=? AND ck2=?", ck1, ck2);
            }

        // partition deletion
        execute("DELETE FROM %s USING TIMESTAMP 160 WHERE pk=1");

        // flush and generate 1 sstable
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        cfs.forceBlockingFlush();
        cfs.disableAutoCompaction();
        cfs.forceMajorCompaction();

        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader reader = cfs.getLiveSSTables().iterator().next();

        try (ISSTableScanner scanner = reader.getScanner())
        {
            try (UnfilteredRowIterator rowIterator = scanner.next())
            {
                // only 1 partition data
                assertFalse(scanner.hasNext());
                List<Unfiltered> expectedUnfiltereds = new ArrayList<>();
                rowIterator.forEachRemaining(expectedUnfiltereds::add);

                // test different throttle
                for (Integer throttle : Arrays.asList(1, 2, 3, 4, 5, 11, 41, 99, 1000, 10001))
                {
                    try (ISSTableScanner scannerForThrottle = reader.getScanner();)
                    {
                        assertTrue(scannerForThrottle.hasNext());
                        try (UnfilteredRowIterator rowIteratorForThrottle = scannerForThrottle.next();)
                        {
                            assertFalse(scannerForThrottle.hasNext());
                            verifyThrottleIterator(expectedUnfiltereds,
                                                   rowIteratorForThrottle,
                                                   new ThrottledUnfilteredIterator(rowIteratorForThrottle, throttle),
                                                   throttle);
                        }
                    }
                }
            }
        }
    }

    private void verifyThrottleIterator(List<Unfiltered> expectedUnfiltereds,
                                        UnfilteredRowIterator rowIteratorForThrottle,
                                        ThrottledUnfilteredIterator throttledIterator,
                                        int throttle)
    {
        List<Unfiltered> output = new ArrayList<>();

        boolean isRevered = rowIteratorForThrottle.isReverseOrder();
        boolean isFirst = true;

        while (throttledIterator.hasNext())
        {
            UnfilteredRowIterator splittedIterator = throttledIterator.next();
            assertMetadata(rowIteratorForThrottle, splittedIterator, isFirst);

            List<Unfiltered> splittedUnfiltereds = new ArrayList<>();
            splittedIterator.forEachRemaining(splittedUnfiltereds::add);

            int remain = expectedUnfiltereds.size() - output.size();
            int expectedSize = remain >= throttle ? throttle : remain;
            if (splittedUnfiltereds.size() != expectedSize)
            {
                assertEquals(expectedSize + 1, splittedUnfiltereds.size());
                // the extra unfilter must be close bound marker
                Unfiltered last = splittedUnfiltereds.get(expectedSize);
                assertTrue(last.isRangeTombstoneMarker());
                RangeTombstoneMarker marker = (RangeTombstoneMarker) last;
                assertFalse(marker.isBoundary());
                assertTrue(marker.isClose(isRevered));
            }
            output.addAll(splittedUnfiltereds);
            if (isFirst)
                isFirst = false;
        }
        int index = 0;
        RangeTombstoneMarker openMarker = null;
        for (int i = 0; i < expectedUnfiltereds.size(); i++)
        {
            Unfiltered expected = expectedUnfiltereds.get(i);
            Unfiltered data = output.get(i);

            // verify that all tombstone are paired
            if (data.isRangeTombstoneMarker())
            {
                RangeTombstoneMarker marker = (RangeTombstoneMarker) data;
                if (marker.isClose(isRevered))
                {
                    assertNotNull(openMarker);
                    openMarker = null;
                }
                if (marker.isOpen(isRevered))
                {
                    assertNull(openMarker);
                    openMarker = marker;
                }
            }
            if (expected.equals(data))
            {
                index++;
            }
            else // because of created closeMarker and openMarker
            {
                assertNotNull(openMarker);
                DeletionTime openDeletionTime = openMarker.openDeletionTime(isRevered);
                // only boundary or row will create extra closeMarker and openMarker
                if (expected.isRangeTombstoneMarker())
                {
                    RangeTombstoneMarker marker = (RangeTombstoneMarker) expected;
                    assertTrue(marker.isBoundary());
                    RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker) marker;
                    assertEquals(boundary.createCorrespondingCloseMarker(isRevered), data);
                    assertEquals(boundary.createCorrespondingOpenMarker(isRevered), output.get(index + 1));
                    assertEquals(openDeletionTime, boundary.endDeletionTime());

                    openMarker = boundary.createCorrespondingOpenMarker(isRevered);
                }
                else
                {
                    ByteBuffer[] byteBuffers = expected.clustering().getRawValues();
                    RangeTombstoneBoundMarker closeMarker = RangeTombstoneBoundMarker.exclusiveClose(isRevered,
                                                                                                     byteBuffers,
                                                                                                     openDeletionTime);

                    RangeTombstoneBoundMarker nextOpenMarker = RangeTombstoneBoundMarker.inclusiveOpen(isRevered,
                                                                                                       byteBuffers,
                                                                                                       openDeletionTime);
                    assertEquals(closeMarker, data);
                    assertEquals(nextOpenMarker, output.get(index + 1));

                    openMarker = nextOpenMarker;
                }
                index += 2;
            }
        }
        assertNull(openMarker);
        assertEquals(output.size(), index);
    }

    @Test
    public void simpleThrottleTest()
    {
        // all live rows with partition deletion
        ThrottledUnfilteredIterator throttledIterator;
        UnfilteredRowIterator origin;

        List<Row> rows = new ArrayList<>();
        int rowCount = 1111;

        for (int i = 0; i < rowCount; i++)
            rows.add(createRow(i, createCell(v1Metadata, i), createCell(v2Metadata, i)));

        // testing different throttle limit
        for (int throttle = 1; throttle < 1200; throttle += 21)
        {
            origin = rows(metadata.regularAndStaticColumns(),
                          1,
                          new DeletionTime(0, 100),
                          Rows.EMPTY_STATIC_ROW,
                          rows.toArray(new Row[0]));
            throttledIterator = new ThrottledUnfilteredIterator(origin, throttle);

            int splittedCount = (int) Math.ceil(rowCount*1.0/throttle);
            for (int i = 1; i <= splittedCount; i++)
            {
                UnfilteredRowIterator splitted = throttledIterator.next();
                assertMetadata(origin, splitted, i == 1);
                // no op
                splitted.close();

                int start = (i - 1) * throttle;
                int end = i == splittedCount ? rowCount : i * throttle;
                assertRows(splitted, rows.subList(start, end).toArray(new Row[0]));
            }
            assertTrue(!throttledIterator.hasNext());
        }
    }

    private void assertMetadata(UnfilteredRowIterator origin, UnfilteredRowIterator splitted, boolean isFirst)
    {
        assertEquals(splitted.columns(), origin.columns());
        assertEquals(splitted.partitionKey(), origin.partitionKey());
        assertEquals(splitted.isReverseOrder(), origin.isReverseOrder());
        assertEquals(splitted.metadata(), origin.metadata());
        assertEquals(splitted.stats(), origin.stats());

        if (isFirst)
        {
            assertEquals(splitted.partitionLevelDeletion(), origin.partitionLevelDeletion());
            assertEquals(splitted.staticRow(), origin.staticRow());
        }
        else
        {
            assertEquals(splitted.partitionLevelDeletion(), DeletionTime.LIVE);
            assertEquals(splitted.staticRow(), Rows.EMPTY_STATIC_ROW);
        }
    }

    public static void assertRows(UnfilteredRowIterator iterator, Row... rows)
    {
        Iterator<Row> rowsIterator = Arrays.asList(rows).iterator();

        while (iterator.hasNext() && rowsIterator.hasNext())
            assertEquals(iterator.next(), rowsIterator.next());

        assertTrue(iterator.hasNext() == rowsIterator.hasNext());
    }

    private static DecoratedKey dk(int pk)
    {
        return new BufferDecoratedKey(new Murmur3Partitioner.LongToken(pk), ByteBufferUtil.bytes(pk));
    }

    private static UnfilteredRowIterator rows(RegularAndStaticColumns columns,
                                              int pk,
                                              DeletionTime partitionDeletion,
                                              Row staticRow,
                                              Unfiltered... rows)
    {
        Iterator<Unfiltered> rowsIterator = Arrays.asList(rows).iterator();
        return new AbstractUnfilteredRowIterator(metadata, dk(pk), partitionDeletion, columns, staticRow, false, EncodingStats.NO_STATS) {
            protected Unfiltered computeNext()
            {
                return rowsIterator.hasNext() ? rowsIterator.next() : endOfData();
            }
        };
    }

    private static Row createRow(int ck, Cell... columns)
    {
        return createRow(ck, ck, columns);
    }

    private static Row createRow(int ck1, int ck2, Cell... columns)
    {
        BTreeRow.Builder builder = new BTreeRow.Builder(true);
        builder.newRow(Util.clustering(metadata.comparator, ck1, ck2));
        for (Cell cell : columns)
            builder.addCell(cell);
        return builder.build();
    }

    private static Cell createCell(ColumnMetadata metadata, int v)
    {
        return createCell(metadata, v, 100L, BufferCell.NO_DELETION_TIME);
    }

    private static Cell createCell(ColumnMetadata metadata, int v, long timestamp, int localDeletionTime)
    {
        return new BufferCell(metadata,
                              timestamp,
                              BufferCell.NO_TTL,
                              localDeletionTime,
                              ByteBufferUtil.bytes(v),
                              null);
    }
}
