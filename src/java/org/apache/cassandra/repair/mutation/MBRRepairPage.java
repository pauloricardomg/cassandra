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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.utils.ByteBufferUtil;

public class MBRRepairPage
{
    public final PartitionPosition firstKey;
    public final PartitionPosition lastKey;
    // note that these come from PagingState.RowMark so they are not serialized using the real types!
    public final ByteBuffer clusteringFrom;
    public final ByteBuffer clusteringTo;
    public final byte[] hash;
    public final int rowCount;
    private final boolean isStartKeyInclusive;

    public MBRRepairPage(PartitionPosition firstKey, PartitionPosition lastKey, ByteBuffer clusteringFrom, ByteBuffer clusteringTo, byte [] hash, int rowCount, boolean isStartKeyInclusive)
    {
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.clusteringFrom = clusteringFrom;
        this.clusteringTo = clusteringTo;
        this.hash = hash;
        this.rowCount = rowCount;
        this.isStartKeyInclusive = isStartKeyInclusive;
    }

    public void serialize(DataOutputPlus out, int version) throws IOException
    {
        PartitionPosition.serializer.serialize(firstKey, out, version);
        PartitionPosition.serializer.serialize(lastKey, out, version);
        ByteBufferUtil.writeWithShortLength(clusteringFrom, out);
        ByteBufferUtil.writeWithShortLength(clusteringTo, out);
        ByteBufferUtil.writeWithShortLength(hash, out);
        out.writeInt(rowCount);
        out.writeBoolean(isStartKeyInclusive);
    }

    public static MBRRepairPage deserialize(DataInputPlus in, IPartitioner partitioner, int version) throws IOException
    {
        PartitionPosition firstKey = PartitionPosition.serializer.deserialize(in, partitioner, version);
        PartitionPosition lastKey = PartitionPosition.serializer.deserialize(in, partitioner, version);
        ByteBuffer clusteringFrom = ByteBufferUtil.readWithShortLength(in);
        ByteBuffer clusteringTo = ByteBufferUtil.readWithShortLength(in);
        short hashLen = in.readShort();
        byte [] hash = new byte[hashLen];
        in.readFully(hash);
        int rowCount = in.readInt();
        boolean isStartKeyInclusive = in.readBoolean();
        return new MBRRepairPage(firstKey, lastKey, clusteringFrom, clusteringTo, hash, rowCount, isStartKeyInclusive);
    }

    public long serializedSize(int version)
    {
        long size = PartitionPosition.serializer.serializedSize(firstKey, version);
        size += PartitionPosition.serializer.serializedSize(lastKey, version);
        size += ByteBufferUtil.serializedSizeWithShortLength(clusteringFrom);
        size += ByteBufferUtil.serializedSizeWithShortLength(clusteringTo);
        size += TypeSizes.sizeof((short)hash.length);
        size += hash.length;
        size += TypeSizes.sizeof(rowCount);
        size += TypeSizes.sizeof(isStartKeyInclusive);
        return size;
    }

    public String toString(CFMetaData metadata)
    {
        return "RepairPage{" +
               ", firstKey = " + firstKey +
               ", lastKey = " + lastKey +
               ", clusteringFrom=" + clusteringString(metadata, clusteringFrom) +
               ", clusteringTo=" + clusteringString(metadata, clusteringTo) +
               ", hash=" + Arrays.toString(hash) +
               ", rowCount=" + rowCount +
               ", inclusiveStartKey=" + isStartKeyInclusive +
               '}';
    }

    private static String clusteringString(CFMetaData metadata, ByteBuffer clustering)
    {
        try
        {
            if (!clustering.equals(ByteBufferUtil.EMPTY_BYTE_BUFFER))
                return Clustering.serializer.deserialize(clustering, MessagingService.current_version, PagingState.RowMark.makeClusteringTypes(metadata)).toString(metadata);
        }
        catch (Throwable t)
        {
            // ignore any error - this is just for logging
        }
        return "<unknown>";
    }

    public PartitionRangeReadCommand createReadCommand(ColumnFamilyStore cfs, int nowInSeconds, int limit)
    {
        Clustering startClustering = clusteringFrom.equals(ByteBufferUtil.EMPTY_BYTE_BUFFER) ? null : Clustering.serializer.deserialize(clusteringFrom, MessagingService.VERSION_30, PagingState.RowMark.makeClusteringTypes(cfs.metadata));
        Clustering stopClustering = clusteringTo.equals(ByteBufferUtil.EMPTY_BYTE_BUFFER) ? null : Clustering.serializer.deserialize(clusteringTo, MessagingService.VERSION_30, PagingState.RowMark.makeClusteringTypes(cfs.metadata));
        Slice.Bound start = startClustering == null ? Slice.Bound.BOTTOM : Slice.Bound.exclusiveStartOf(startClustering);
        Slice.Bound end = stopClustering == null ? Slice.Bound.TOP : Slice.Bound.inclusiveEndOf(stopClustering);
        AbstractBounds<PartitionPosition> bounds = AbstractBounds.bounds(firstKey, isStartKeyInclusive, lastKey, true);
        DataRange dr = new MBRService.MBRDataRange(bounds, cfs.getComparator(), start, end);
        DataLimits dataLimit = DataLimits.cqlLimits(limit);
        return new PartitionRangeReadCommand(cfs.metadata,
                                             nowInSeconds,
                                             ColumnFilter.all(cfs.metadata),
                                             RowFilter.NONE,
                                             dataLimit,
                                             dr,
                                             Optional.empty());
    }
}