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
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;


/**
 * Handles the MutationBasedRepairCommand
 *
 * We get a RepairPage from another node, we figure out what we should read based on the RepairPage and we
 * hash that data. Then we compare the hashes and if the hashes don't match, we read up the data and return that
 * to the remote node. If the number of rows is too large, we return a "HUGE" response and let the repairing node
 * page the data back.
 */
public class MBRVerbHandler implements IVerbHandler<MBRCommand>
{
    private static final Logger logger = LoggerFactory.getLogger(MBRVerbHandler.class);

    public void doVerb(MessageIn<MBRCommand> message, int id) throws IOException
    {
        MBRCommand mbrc = message.payload;
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(message.payload.cfid);
        assert cfs != null;
        PartitionRangeReadCommand rc = mbrc.repairPage.createReadCommand(cfs, mbrc.nowInSeconds, mbrc.repairPage.windowSize * 2);
        boolean match = true;
        long rowCount = 0;
        try (ReadExecutionController rce = rc.executionController();
             UnfilteredPartitionIterator pi = rc.executeLocally(rce))
        {
            DataLimits.Counter c = rc.limits().newCounter(mbrc.nowInSeconds, true);
            byte[] hash = MBRService.digest(rc, c.applyTo(pi)).right;
            rowCount = c.counted();
            if (!Arrays.equals(hash, mbrc.repairPage.hash))
                match = false;
        }
        MBRResponse response;
        if (rowCount >= mbrc.repairPage.windowSize * 2) // this means the limit was hit
        {
            response = MBRResponse.HUGE;
        }
        else if (!match)
        {
            try (ReadExecutionController rce = rc.executionController();
                 UnfilteredPartitionIterator localData = rc.executeLocally(rce))
            {
                ReadResponse localResponse = rc.createResponse(localData);
                response = new MBRResponse(localResponse, MBRResponse.Type.DATA);
            }
        }
        else
        {
            response = MBRResponse.MATCH;
        }

        MessagingService.instance().sendReply(response.createMessage(), id, message.from);
    }
}