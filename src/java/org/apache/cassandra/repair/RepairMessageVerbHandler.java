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
package org.apache.cassandra.repair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.repair.messages.*;
import org.apache.cassandra.service.ActiveRepairService;

/**
 * Handles all repair related message.
 *
 * @since 2.0
 */
public class RepairMessageVerbHandler implements IVerbHandler<RepairMessage>
{
    private static final Logger logger = LoggerFactory.getLogger(RepairMessageVerbHandler.class);

    private static final ActiveRepairService repairService = ActiveRepairService.instance;

    public void doVerb(final MessageIn<RepairMessage> message, final int id)
    {
        RepairJobDesc desc = message.payload.desc;
        try
        {
            switch (message.payload.messageType)
            {
                case PREPARE_MESSAGE:
                    PrepareMessage prepareMessage = (PrepareMessage) message.payload;
                    logger.debug("Preparing, {}", prepareMessage);
                    repairService.doPrepare(prepareMessage.parentRepairSession, message.from, id, prepareMessage.cfIds, prepareMessage.ranges,
                                            prepareMessage.isIncremental, prepareMessage.timestamp, prepareMessage.isGlobal);
                    break;

                case SNAPSHOT:
                    logger.debug("Snapshotting {}", desc);
                    repairService.doSnapshot(desc.parentSessionId, desc.sessionId, message.from, id,
                                             desc.keyspace, desc.columnFamily, desc.ranges);
                    break;

                case VALIDATION_REQUEST:
                    ValidationRequest validationRequest = (ValidationRequest) message.payload;
                    logger.debug("Validating {}", validationRequest);
                    repairService.doValidate(desc.parentSessionId, desc.sessionId, message.from, desc, validationRequest.gcBefore);
                    break;

                case SYNC_REQUEST:
                    // forwarded sync request
                    SyncRequest request = (SyncRequest) message.payload;
                    logger.debug("Syncing {}", request);
                    repairService.doSync(desc.parentSessionId, desc.sessionId, message.from, desc, request);
                    break;

                case ANTICOMPACTION_REQUEST:
                    AnticompactionRequest anticompactionRequest = (AnticompactionRequest) message.payload;
                    logger.debug("Got anticompaction request {}", anticompactionRequest);
                    repairService.doRemoteAntiCompaction(anticompactionRequest.parentRepairSession, message.from, id, anticompactionRequest.successfulRanges);
                    break;

                case CLEANUP:
                    logger.debug("cleaning up repair");
                    CleanupMessage cleanup = (CleanupMessage) message.payload;
                    repairService.doCleanup(cleanup.parentRepairSession, message.from, id);
                    break;

                case ABORT:
                    logger.debug("Received abort message from {}", message.from);
                    AbortMessage abort = (AbortMessage) message.payload;
                    repairService.doAbort(abort.parentRepairSession, message.from);
                    break;

                case VALIDATION_COMPLETE:
                    ValidationComplete validation = (ValidationComplete) message.payload;
                    logger.debug("Received validation complete message from {}: {}", message.from, validation);
                    repairService.doneValidation(message.from, desc, validation.trees);
                    break;

                case SYNC_COMPLETE:
                    // one of replica is synced.
                    SyncComplete sync = (SyncComplete) message.payload;
                    logger.debug("Received sync complete message from {}: {}", message.from, sync);
                    repairService.doneSync(desc, sync.nodes, sync.success);
                    break;
            }
        }
        catch (Exception e)
        {
            logger.error("Got error, removing parent repair session");
            if (desc != null && desc.parentSessionId != null)
                ActiveRepairService.instance.removeParentRepairSession(desc.parentSessionId);
            throw new RuntimeException(e);
        }
    }
}
