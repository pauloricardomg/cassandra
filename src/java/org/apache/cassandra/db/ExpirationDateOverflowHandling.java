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

package org.apache.cassandra.db;

import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.NoSpamLogger;

public class ExpirationDateOverflowHandling
{
    private static final Logger logger = LoggerFactory.getLogger(Attributes.class);

    public enum ExpirationDateOverflowPolicy
    {
        REJECT, CAP_NOWARN, CAP_WARN
    }

    @VisibleForTesting
    public static ExpirationDateOverflowPolicy expirationDateOverflowPolicy;

    public static boolean recoverOverflowedExpiration = Boolean.valueOf(System.getProperty("cassandra.recover_overflowed_expiration_best_effort", "false"));

    static {
        String policyAsString = System.getProperty("cassandra.expiration_date_overflow_policy", ExpirationDateOverflowPolicy.REJECT.name());
        try
        {
            expirationDateOverflowPolicy = ExpirationDateOverflowPolicy.valueOf(policyAsString.toUpperCase());
        }
        catch (RuntimeException e)
        {
            logger.debug("Invalid expiration date overflow policy: {}. Using default: {}", policyAsString, ExpirationDateOverflowPolicy.REJECT.name());
            expirationDateOverflowPolicy = ExpirationDateOverflowPolicy.REJECT;
        }
    }

    public static final String MAXIMUM_EXPIRATION_DATE_EXCEEDED_WARNING = "Request on table {}.{} with {}ttl of {} seconds exceeds maximum supported expiration " +
                                                                          "date of 2038-01-19T03:14:06+00:00 and will have its expiration capped to that date. " +
                                                                          "In order to avoid this use a lower TTL or upgrade to a version where this limitation " +
                                                                          "is fixed. See CASSANDRA-14092 for more details.";

    public static final String MAXIMUM_EXPIRATION_DATE_EXCEEDED_REJECT_MESSAGE = "Request on table %s.%s with %sttl of %d seconds exceeds maximum supported expiration " +
                                                                                 "date of 2038-01-19T03:14:06+00:00. In order to avoid this use a lower TTL, change " +
                                                                                 "the expiration date overflow policy or upgrade to a version where this limitation " +
                                                                                 "is fixed. See CASSANDRA-14092 for more details.";

    public static void maybeApplyExpirationDateOverflowPolicy(CFMetaData metadata, int ttl, boolean isDefaultTTL) throws InvalidRequestException
    {
        if (ttl == 0)
            return;

        // Check for localExpirationTime overflow (CASSANDRA-14092)
        int nowInSecs = (int)(System.currentTimeMillis() / 1000);
        if (ttl + nowInSecs < 0)
        {
            switch (expirationDateOverflowPolicy)
            {
                case CAP_WARN:
                    NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES, MAXIMUM_EXPIRATION_DATE_EXCEEDED_WARNING,
                                     metadata.ksName, metadata.cfName, isDefaultTTL? "default " : "", ttl);
                case CAP_NOWARN:
                    /**
                     * Capping at this stage is basically not rejecting the request. The actual capping is done
                     * by {@link org.apache.cassandra.db.BufferExpiringCell#sanitizeLocalExpirationTime(int)},
                     * which converts the negative TTL to {@link org.apache.cassandra.db.BufferExpiringCell#MAX_DELETION_TIME}
                     */
                    return;

                default:
                    throw new InvalidRequestException(String.format(MAXIMUM_EXPIRATION_DATE_EXCEEDED_REJECT_MESSAGE, metadata.ksName, metadata.cfName,
                                                                    isDefaultTTL? "default " : "", ttl));
            }
        }
    }

    /**
     * The {@link org.apache.cassandra.db.rows.BufferCell#localDeletionTime} overflows when its value is negative
     * or equal to {@link Integer#MAX_VALUE}, which is used to represented {@link org.apache.cassandra.db.rows.BufferCell#NO_DELETION_TIME}.
     *
     * This method recovers overflowed {@link org.apache.cassandra.db.rows.BufferCell#localDeletionTime} setting it
     * to the maximum representable value which is {@link Cell#MAX_DELETION_TIME}, iff the system property
     * cassandra.recover_overflowed_expiration_best_effort is true.
     *
     * See CASSANDRA-14092
     */
    public static int maybeRecoverOverflowedExpiration(int ttl, int localExpirationTime)
    {
        if (!recoverOverflowedExpiration)
            return localExpirationTime;

        if (ttl != BufferCell.NO_TTL && (localExpirationTime < 0 || localExpirationTime == BufferCell.NO_DELETION_TIME))
            return Cell.MAX_DELETION_TIME;

        return localExpirationTime;
    }

    /**
     * This method computes the {@link Cell#localDeletionTime()}, maybe capping to the maximum representable value
     * which is {@link Cell#MAX_DELETION_TIME}.
     *
     * Please note that the {@link ExpirationDateOverflowHandling.ExpirationDateOverflowPolicy} is applied
     * during {@link ExpirationDateOverflowHandling#maybeApplyExpirationDateOverflowPolicy(CFMetaData, int, boolean)},
     * so if the request was not denied it means it's expiration date should be capped.
     *
     * See CASSANDRA-14092
     */
    public static int computeLocalExpirationTime(int nowInSec, int timeToLive)
    {
        int localExpirationTime =  nowInSec + timeToLive;
        return localExpirationTime >= 0? localExpirationTime : Cell.MAX_DELETION_TIME;
    }

}
