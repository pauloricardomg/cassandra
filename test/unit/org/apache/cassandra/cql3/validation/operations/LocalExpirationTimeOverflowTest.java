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

package org.apache.cassandra.cql3.validation.operations;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

public class LocalExpirationTimeOverflowTest extends CQLTester
{
    public static String NEGATIVE_LOCAL_EXPIRATION_TEST_DIR = "test/data/negative-local-expiration-test/%s";

    public static final String KEYSPACE = "keyspace1";
    public static final String SIMPLE_NOCLUSTERING = "table1";
    public static final String SIMPLE_CLUSTERING = "table2";
    public static final String COMPLEX_NOCLUSTERING = "table3";
    public static final String COMPLEX_CLUSTERING = "table4";

    @BeforeClass
    public static void beforeClass()
    {
        CQLTester.setUpClass();
        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1));
    }

    @Before
    public void before() throws Throwable
    {
        execute("USE " + KEYSPACE);
        execute("create table IF NOT EXISTS " + SIMPLE_CLUSTERING + " (k int, a int, b int, primary key(k, a))");
        execute("create table IF NOT EXISTS " + SIMPLE_NOCLUSTERING + " (k int primary key, a int, b int)");
        execute("create table IF NOT EXISTS "  + COMPLEX_CLUSTERING + " (k int, a int, b set<text>, primary key(k, a))");
        execute("create table IF NOT EXISTS "  + COMPLEX_NOCLUSTERING + " (k int primary key, a int, b set<text>)");
    }

    @After
    public void after()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        keyspace.getColumnFamilyStore(SIMPLE_NOCLUSTERING).truncateBlocking();
        keyspace.getColumnFamilyStore(SIMPLE_CLUSTERING).truncateBlocking();
        keyspace.getColumnFamilyStore(COMPLEX_NOCLUSTERING).truncateBlocking();
        keyspace.getColumnFamilyStore(COMPLEX_CLUSTERING).truncateBlocking();
    }

    @Test
    public void testLocalExpirationTimeOverflowSimpleNoClusteringWithFlush() throws Throwable
    {
        testLocalExpirationTimeOverflowSimple(false,true);
    }

    @Test
    public void testLocalExpirationTimeOverflowSimpleNoClusteringWithoutFlush() throws Throwable
    {
        testLocalExpirationTimeOverflowSimple(false, false);
    }

    @Test
    public void testLocalExpirationTimeOverflowSimpleClusteringWithFlush() throws Throwable
    {
        testLocalExpirationTimeOverflowSimple(true,true);
    }

    @Test
    public void testLocalExpirationTimeOverflowSimpleClusteringWithoutFlush() throws Throwable
    {
        testLocalExpirationTimeOverflowComplex(true, false);
    }

    @Test
    public void testLocalExpirationTimeOverflowComplexNoClusteringWithFlush() throws Throwable
    {
        testLocalExpirationTimeOverflowComplex(false,true);
    }

    @Test
    public void testLocalExpirationTimeOverflowComplexNoClusteringWithoutFlush() throws Throwable
    {
        testLocalExpirationTimeOverflowComplex(false, false);
    }

    @Test
    public void testLocalExpirationTimeOverflowComplexClusteringWithFlush() throws Throwable
    {
        testLocalExpirationTimeOverflowComplex(true,true);
    }

    @Test
    public void testLocalExpirationTimeOverflowComplexClusteringWithoutFlush() throws Throwable
    {
        testLocalExpirationTimeOverflowComplex(true, false);
    }

    public void testLocalExpirationTimeOverflowSimple(boolean clustering, boolean flush) throws Throwable
    {
        String tableName = getTableName(true, clustering);
        execute("INSERT INTO " + tableName + " (k, a, b) VALUES (?, ?, ?) USING TTL 630720000", 2, 2, 2);
        if (clustering)
            execute("UPDATE " + tableName + " USING TTL 630720000 SET b = 1 WHERE k = 1 AND a = 1;");
        else
            execute("UPDATE " + tableName + " USING TTL 630720000 SET a = 1, b = 1 WHERE k = 1;");

        Keyspace ks = Keyspace.open(KEYSPACE);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRows(execute("SELECT * from " + tableName), row(1, 1, 1), row(2, 2, 2));
    }

    public void testLocalExpirationTimeOverflowComplex(boolean clustering, boolean flush) throws Throwable
    {
        execute("USE " + KEYSPACE);

        String tableName = getTableName(false, clustering);
        execute("INSERT INTO " + tableName + " (k, a, b) VALUES (?, ?, ?) USING TTL 630720000", 2, 2, set("v21", "v22", "v23", "v24"));
        if (clustering)
            execute("UPDATE  " + tableName + " USING TTL 630720000 SET b = ? WHERE k = 1 AND a = 1;", set("v11", "v12", "v13", "v14"));
        else
            execute("UPDATE  " + tableName + " USING TTL 630720000 SET a = 1, b = ? WHERE k = 1;", set("v11", "v12", "v13", "v14"));

        Keyspace ks = Keyspace.open(KEYSPACE);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRows(execute("SELECT * from " + tableName), row(1, 1, set("v11", "v12", "v13", "v14")), row(2, 2, set("v21", "v22", "v23", "v24")));
    }

    @Test
    public void testLocalExpirationTimeOverflowSimpleNoClusteringLegacy() throws Throwable
    {
        testLocalExpirationTimeOverflowLegacy(true, false);
    }

    @Test
    public void testLocalExpirationTimeOverflowSimpleClusteringLegacy() throws Throwable
    {
        testLocalExpirationTimeOverflowLegacy(true, true);
    }

    @Test
    public void testLocalExpirationTimeOverflowComplexNoClusteringLegacy() throws Throwable
    {
        testLocalExpirationTimeOverflowLegacy(false, false);
    }

    @Test
    public void testLocalExpirationTimeOverflowComplexClusteringLegacy() throws Throwable
    {
        testLocalExpirationTimeOverflowLegacy(false, true);
    }

    public void testLocalExpirationTimeOverflowLegacy(boolean simple, boolean clustering) throws Throwable
    {
        String tableName = getTableName(simple, clustering);
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(tableName);

        assertEquals(0, cfs.getLiveSSTables().size());

        copySSTablesToTableDir(tableName);

        cfs.loadNewSSTables();

        if (simple)
            assertRows(execute("SELECT * from " + tableName), row(1, 1, 1), row(2, 2, 2));
        else
            assertRows(execute("SELECT * from " + tableName), row(1, 1, set("v11", "v12", "v13", "v14")), row(2, 2, set("v21", "v22", "v23", "v24")));
    }

    public String getTableName(boolean simple, boolean clustering)
    {
        if (simple)
            return clustering ? SIMPLE_CLUSTERING : SIMPLE_NOCLUSTERING;
        else
            return clustering ? COMPLEX_CLUSTERING : COMPLEX_NOCLUSTERING;
    }

    private static void copySSTablesToTableDir(String table) throws IOException
    {
        File tableDir = Keyspace.open(KEYSPACE).getColumnFamilyStore(table).getDirectories().getCFDirectories().iterator().next();
        File tableDir1 = getTableDir(table);
        for (File file : tableDir1.listFiles())
        {
            copyFile(file, tableDir);
        }
    }

    private static File getTableDir(String table)
    {
        return new File(String.format(NEGATIVE_LOCAL_EXPIRATION_TEST_DIR, table));
    }

    private static void copyFile(File src, File dest) throws IOException
    {
        byte[] buf = new byte[65536];
        if (src.isFile())
        {
            File target = new File(dest, src.getName());
            int rd;
            FileInputStream is = new FileInputStream(src);
            FileOutputStream os = new FileOutputStream(target);
            while ((rd = is.read(buf)) >= 0)
                os.write(buf, 0, rd);
        }
    }
}
