package org.apache.cassandra.cql;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.ExpiringCell;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ThriftClientState;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Iterator;

import static org.apache.cassandra.Util.token;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test that {@link org.apache.cassandra.cql3.Attributes.ExpirationDateOverflowPolicy} works properly with CQLv2 INSERTS.
 *
 * This suite checks that INSERTS without TTL on CQLv2 work correctly, and that the expiration overflow policy is
 * applied on CQLv2 INSERTS with TTL.
 */
public class CQLv2TTLTest extends SchemaLoader
{
    private static UntypedResultSet execute(String query) throws Throwable
    {
        try
        {
            return org.apache.cassandra.cql3.QueryProcessor.executeInternal(String.format(query));
        }
        catch (RuntimeException exc)
        {
            if (exc.getCause() != null)
                throw exc.getCause();
            throw exc;
        }
    }

    @After
    public void tearDown() throws Throwable
    {
        execute("TRUNCATE \"Keyspace1\".\"Standard1\"");
    }

    @Test
    public void testSuccessfulInsertCqlv2() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.updateNormalToken(token("1"), InetAddress.getByName("127.0.0.1"));

        // Insert data with CQLv2
        ThriftClientState clientState = new ThriftClientState(InetSocketAddress.createUnresolved("127.0.0.1", 7000));
        clientState.setKeyspace("Keyspace1");
        QueryProcessor.process("INSERT INTO 'Standard1' (KEY, 61) VALUES (61,61);", clientState);

        // Check data with CQLv3
        UntypedResultSet results = execute("SELECT key, column1 FROM \"Keyspace1\".\"Standard1\"");
        assertEquals(1, results.size());
        UntypedResultSet.Row row = results.iterator().next();
        assertEquals(ByteBufferUtil.hexToBytes("61"), row.getBytes("key"));
    }

    @Test
    public void testSuccessfulInsertBatchCqlv2() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.updateNormalToken(token("1"), InetAddress.getByName("127.0.0.1"));

        // Insert data with CQLv2
        ThriftClientState clientState = new ThriftClientState(InetSocketAddress.createUnresolved("127.0.0.1", 7000));
        clientState.setKeyspace("Keyspace1");
        QueryProcessor.process("BEGIN BATCH " +
                               "INSERT INTO 'Standard1' (KEY, 61) VALUES (61, 61); " +
                               "INSERT INTO 'Standard1' (KEY, 62) VALUES (62, 62); " +
                               "APPLY BATCH;", clientState);

        // Check data with CQLv3
        UntypedResultSet results = execute("SELECT key, column1 FROM \"Keyspace1\".\"Standard1\"");
        assertEquals(2, results.size());
        Iterator<UntypedResultSet.Row> iterator = results.iterator();
        UntypedResultSet.Row row1 = iterator.next();
        assertEquals(ByteBufferUtil.hexToBytes("61"), row1.getBytes("key"));
        UntypedResultSet.Row row2 = iterator.next();
        assertEquals(ByteBufferUtil.hexToBytes("62"), row2.getBytes("key"));
    }

    @Test
    public void testRejectExpirationOverflowPolicyInsertCqlV2() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.updateNormalToken(token("1"), InetAddress.getByName("127.0.0.1"));

        // Insert data with CQLv2 - should not insert
        ThriftClientState clientState = new ThriftClientState(InetSocketAddress.createUnresolved("127.0.0.1", 7000));
        clientState.setKeyspace("Keyspace1");
        try
        {
            QueryProcessor.process("INSERT INTO 'Standard1' (KEY, 61) VALUES (61,61) USING TTL " + ExpiringCell.MAX_TTL, clientState);
            fail("Should have thrown RuntimeException");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getMessage().contains("Request on table Keyspace1.Standard1 with ttl of 630720000 seconds exceeds maximum supported expiration date"));
        }

        // Check data with CQLv3 - should be empty
        UntypedResultSet results = execute("SELECT key, column1 FROM \"Keyspace1\".\"Standard1\"");
        assertEquals(0, results.size());
    }

    @Test
    public void testRejectExpirationOverflowPolicyInsertBatchCqlV2() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.updateNormalToken(token("1"), InetAddress.getByName("127.0.0.1"));

        ThriftClientState clientState = new ThriftClientState(InetSocketAddress.createUnresolved("127.0.0.1", 7000));
        clientState.setKeyspace("Keyspace1");
        try
        {
            QueryProcessor.process("BEGIN BATCH " +
                                   "INSERT INTO 'Standard1' (KEY, 61) VALUES (61,61); " +
                                   "INSERT INTO 'Standard1' (KEY, 62) VALUES (62,62) USING TTL 630720000; " +
                                   "APPLY BATCH;", clientState);
            fail("Should have thrown RuntimeException");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getMessage().contains("Request on table Keyspace1.Standard1 with ttl of 630720000 seconds exceeds maximum supported expiration date"));
        }

        // Check data with CQLv3 - should be empty
        UntypedResultSet results = execute("SELECT key, column1 FROM \"Keyspace1\".\"Standard1\"");
        assertEquals(0, results.size());
    }

    @Test
    public void testCapExpirationOverflowPolicyInsertCqlV2() throws Throwable
    {
        org.apache.cassandra.cql3.Attributes.policy = org.apache.cassandra.cql3.Attributes.ExpirationDateOverflowPolicy.CAP;
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.updateNormalToken(token("1"), InetAddress.getByName("127.0.0.1"));

        // Insert data with CQLv2 - insert should be successful
        ThriftClientState clientState = new ThriftClientState(InetSocketAddress.createUnresolved("127.0.0.1", 7000));
        clientState.setKeyspace("Keyspace1");
        QueryProcessor.process("INSERT INTO 'Standard1' (KEY, 61) VALUES (61,61) USING TTL " + ExpiringCell.MAX_TTL, clientState);

        // Check data with CQLv3
        UntypedResultSet results = execute("SELECT key, column1 FROM \"Keyspace1\".\"Standard1\"");
        assertEquals(1, results.size());
        UntypedResultSet.Row row = results.iterator().next();
        assertEquals(ByteBufferUtil.hexToBytes("61"), row.getBytes("key"));
    }

    @Test
    public void testCapExpirationOverflowPolicyInsertBatchCqlV2() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.updateNormalToken(token("1"), InetAddress.getByName("127.0.0.1"));

        // Insert data with CQLv2 - insert should be successful
        ThriftClientState clientState = new ThriftClientState(InetSocketAddress.createUnresolved("127.0.0.1", 7000));
        clientState.setKeyspace("Keyspace1");
        QueryProcessor.process("BEGIN BATCH " +
                               "INSERT INTO 'Standard1' (KEY, 61) VALUES (61,61); " +
                               "INSERT INTO 'Standard1' (KEY, 62) VALUES (62,62) USING TTL 630720000; " +
                               "APPLY BATCH;", clientState);

        // Check data with CQLv3
        UntypedResultSet results = execute("SELECT key, column1 FROM \"Keyspace1\".\"Standard1\"");
        assertEquals(2, results.size());
        Iterator<UntypedResultSet.Row> iterator = results.iterator();
        UntypedResultSet.Row row1 = iterator.next();
        assertEquals(ByteBufferUtil.hexToBytes("61"), row1.getBytes("key"));
        UntypedResultSet.Row row2 = iterator.next();
        assertEquals(ByteBufferUtil.hexToBytes("62"), row2.getBytes("key"));
    }
}
