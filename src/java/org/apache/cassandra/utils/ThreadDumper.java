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

package org.apache.cassandra.utils;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When enabled, it logs a full thread dump once upon invocation
 * of <code>maybeLogThreadDump()</code> and disables itself
 */
public class ThreadDumper implements ThreadDumperMBean
{
    protected static final Logger logger = LoggerFactory.getLogger(ThreadDumper.class);

    private AtomicBoolean enabled = new AtomicBoolean(false);

    public void init(String name)
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        String mbeanName = "org.apache.cassandra.utils.ThreadDumper:type=" + name;
        try
        {
            mbs.registerMBean(this, new ObjectName(mbeanName));
        }
        catch (Exception e)
        {
            logger.warn("Could not register thread dumper mbean: {}. Thread dumping functionality will be unavailable.", mbeanName);
        }
    }

    /**
     * Should be called when there is a contention where thread dump should be logged if functionality is enabled
     */
    public void maybeLogThreadDump()
    {
        if (enabled.get() && enabled.compareAndSet(true, false))
        {
            StringBuilder threadDumpBuilder = new StringBuilder();
            Arrays.stream(ManagementFactory.getThreadMXBean().dumpAllThreads(true, true)).forEach(i -> threadDumpBuilder.append(i));
            logger.info("Logging full thread dump due to contention:{}{}", System.lineSeparator(), threadDumpBuilder.toString());
        }
    }

    public void setEnableThreadDumpOnNextContention(boolean enabled)
    {
        this.enabled.set(enabled);
    }

    public boolean getEnableThreadDumpOnNextContention()
    {
        return enabled.get();
    }
}
