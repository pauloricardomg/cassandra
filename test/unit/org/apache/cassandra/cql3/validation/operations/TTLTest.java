package org.apache.cassandra.cql3.validation.operations;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.BufferExpiringCell;
import org.apache.cassandra.db.ExpiringCell;
import org.apache.cassandra.exceptions.InvalidRequestException;

import org.junit.Test;

public class TTLTest extends CQLTester
{
    public static final int MAX_TTL = ExpiringCell.MAX_TTL;

    @Test
    public void testTTLPerRequestLimit() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int)");
        // insert
        execute("INSERT INTO %s (k, i) VALUES (1, 1) USING TTL ?", MAX_TTL); // max ttl
        checkMaxTTL();

        try
        {
            execute("INSERT INTO %s (k, i) VALUES (1, 1) USING TTL ?", MAX_TTL + 1);
            fail("Expect InvalidRequestException");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains("ttl is too large."));
        }

        try
        {
            execute("INSERT INTO %s (k, i) VALUES (1, 1) USING TTL ?", -1);
            fail("Expect InvalidRequestException");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains("A TTL must be greater or equal to 0"));
        }
        execute("TRUNCATE %s");

        // update
        execute("UPDATE %s USING TTL ? SET i = 1 WHERE k = 2", MAX_TTL); // max ttl
        checkMaxTTL();

        try
        {
            execute("UPDATE %s USING TTL ? SET i = 1 WHERE k = 2", MAX_TTL + 1);
            fail("Expect InvalidRequestException");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains("ttl is too large."));
        }

        try
        {
            execute("UPDATE %s USING TTL ? SET i = 1 WHERE k = 2", -1);
            fail("Expect InvalidRequestException");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains("A TTL must be greater or equal to 0"));
        }
    }

    /**
     * Verify that the computed TTL is equal to the maximum allowed ttl given the
     * {@link ExpiringCell#getLocalDeletionTime()} field limitation (CASSANDRA-14092)
     */
    private void checkMaxTTL() throws Throwable
    {
        // Since the max TTL is dynamic, we compute if before and after the query to avoid flakiness
        int minTTL = computeMaxTTL();
        int ttl = execute("SELECT ttl(i) FROM %s").one().getInt("ttl(i)");
        int maxTTL = computeMaxTTL();
        assertTrue(minTTL >= ttl &&  ttl <= maxTTL);
    }

    /**
     * The max TTL is computed such that the TTL summed with the current time is equal to the maximum
     * allowed expiration time {@link BufferExpiringCell#getLocalDeletionTime()} (2038-01-19T03:14:06+00:00)
     */
    private int computeMaxTTL()
    {
        int nowInSecs = (int) (System.currentTimeMillis() / 1000);
        return BufferExpiringCell.MAX_DELETION_TIME - nowInSecs;
    }

    @Test
    public void testTTLDefaultLimit() throws Throwable
    {
        try
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, i int) WITH default_time_to_live=-1");
            fail("Expect Invalid schema");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getCause()
                        .getCause()
                        .getMessage()
                        .contains("default_time_to_live cannot be smaller than 0"));
        }
        try
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, i int) WITH default_time_to_live="
                    + (MAX_TTL + 1));
            fail("Expect Invalid schema");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getCause()
                        .getCause()
                        .getMessage()
                        .contains("default_time_to_live must be less than or equal to " + MAX_TTL + " (got "
                                + (MAX_TTL + 1) + ")"));
        }

        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int) WITH default_time_to_live=" + MAX_TTL);
        execute("INSERT INTO %s (k, i) VALUES (1, 1)");
        checkMaxTTL();
    }

}
