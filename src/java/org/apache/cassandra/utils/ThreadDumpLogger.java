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

public interface ThreadDumpLogger
{
    /**
     * Logs full thread dump on next contention faced by this object.
     *
     * If enabled, the implementing class should define what contention scenario should log a
     * thread dump.
     *
     * After a contention event happens and a thread dump is logged, the flag is automatically
     * disabled to prevent logging multiple thread dumps in an overloaded scenario and
     * must be manually re-enabled aferwards if necessary.
     *
     * @param enabled flag to enable or disable thread dump logging on contention
     */
    void setLogThreadDumpOnNextContention(boolean enabled);

    /**
     * @return whether thread dump logging is enabled
     */
    boolean getLogThreadDumpOnNextContention();
}
