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

import java.io.Closeable;
import java.util.Map;

import org.slf4j.MDC;

public final class LogContext
{

    private static final String COLUMN_FAMILY = "cf";
    private static final String KEYSPACE = "ks";

    public static final void setKeyspace(String ks)
    {
        if (ks != null)
            MDC.put(KEYSPACE, String.format("|%s", ks));
    }

    public static final void setColumnFamily(String cf)
    {
        if (cf != null)
            MDC.put(COLUMN_FAMILY, String.format(".%s", cf));
    }

    public static final void setKeyspaceAndColumnFamily(String ks, String cf)
    {
        setKeyspace(ks);
        setColumnFamily(cf);
    }

    public static final void clear()
    {
        MDC.clear();
    }

    /**
     * Try-with-resources logging wrapper.
     *
     * All logging statements within the wrapper will display
     * the keyspace and column family given in the constructor.
     *
     * This ensures the logging context is cleared after
     * the block within the wrapper finished.
     */
    public static final class Wrapper implements Closeable
    {
        public Wrapper(String keyspace, String columnFamily)
        {
            setKeyspaceAndColumnFamily(keyspace, columnFamily);
        }

        public void close()
        {
            clear();
        }
    }

    /**
     * Inherits logging context from parent thread
     */
    public static abstract class Runnable implements java.lang.Runnable
    {
        private final Map mdcContext = MDC.getCopyOfContextMap();

        @Override
        public final void run() {
            if (mdcContext != null)
                MDC.setContextMap(mdcContext);
            try
            {
                runWithLogContext();
            }
            finally
            {
                if (mdcContext != null)
                    clear();
            }
        }

        protected abstract void runWithLogContext();
    }
}
