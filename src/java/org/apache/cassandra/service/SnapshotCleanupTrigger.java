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
package org.apache.cassandra.service;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SnapshotDetails;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.config.DatabaseDescriptor;
import java.io.File;
import java.util.List;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.CompositeData;
import java.util.Map;
import java.time.Instant;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;


public class SnapshotCleanupTrigger implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotCleanupTrigger.class);

    public void run() {
        logger.info("start cleanup");

        for (Map.Entry<String, TabularData> entry : StorageService.instance.getSnapshotDetails().entrySet()) {
            String snapshotName = entry.getKey();
            TabularData data = entry.getValue();

            for (Object raw : data.values()) {
                CompositeData row = (CompositeData)raw;
                String expiresAt = (String)row.get("Time of snapshot expiration");
                if (isExpired(expiresAt)) {
                    logger.info("kek {}", row.containsKey("Time of snapshot expiration"));
                    Keyspace.clearSnapshot(snapshotName, (String)row.get("Keyspace name"));
                }
            }
        }
    }

    private boolean isExpired(String expiresAt) {
        if (expiresAt == null) {
            return false;
        }
        Instant expiration = Instant.parse(expiresAt);
        Instant now = Instant.now();
        return expiration.compareTo(now) < 0;
    }

}