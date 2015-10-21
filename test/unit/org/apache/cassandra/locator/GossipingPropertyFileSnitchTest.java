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

package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import com.google.common.net.InetAddresses;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for {@link GossipingPropertyFileSnitch}.
 */
public class GossipingPropertyFileSnitchTest
{
    @Before
    public void setup()
    {
        //Init gossiper local state
        Gossiper.instance.maybeInitializeLocalState(0);
    }

    @After
    public void tearDown()
    {
        Gossiper.instance.resetEndpointStateMap();
    }

    public static void checkEndpoint(final AbstractNetworkTopologySnitch snitch,
                                     final String endpointString, final String expectedDatacenter,
                                     final String expectedRack)
    {
        final InetAddress endpoint = InetAddresses.forString(endpointString);
        assertEquals(expectedDatacenter, snitch.getDatacenter(endpoint));
        assertEquals(expectedRack, snitch.getRack(endpoint));
    }

    @Test
    public void testAutoReloadConfig() throws Exception
    {
        String confFile = FBUtilities.resourceToFile(SnitchProperties.RACKDC_PROPERTY_FILENAME);
        
        final GossipingPropertyFileSnitch snitch = new GossipingPropertyFileSnitch(/*refreshPeriodInSeconds*/1);
        checkEndpoint(snitch, FBUtilities.getBroadcastAddress().getHostAddress(), "DC1", "RAC1");

        //prefer_local=false; local_address=127.0.0.2, so gossiper internal ip should be null
        assertNull(getInternalIp());

        final Path effectiveFile = Paths.get(confFile);
        final Path backupFile = Paths.get(confFile + ".bak");
        final Path modifiedFile = Paths.get(confFile + ".mod");

        try
        {
            Files.copy(effectiveFile, backupFile);
            Files.copy(modifiedFile, effectiveFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            
            Thread.sleep(1500);
            
            checkEndpoint(snitch, FBUtilities.getBroadcastAddress().getHostAddress(), "DC2", "RAC2");
            assertEquals("127.0.0.2", getInternalIp().value);
        }
        finally
        {
            Files.copy(backupFile, effectiveFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            Files.delete(backupFile);
        }
    }

    @Test
    public void testPreferLocalSpecifyingLocalAddress() throws Exception
    {
        //Define configuration files
        String confFile = FBUtilities.resourceToFile(SnitchProperties.RACKDC_PROPERTY_FILENAME);
        final Path effectiveFile = Paths.get(confFile);
        final Path backupFile = Paths.get(confFile + ".bak");
        final Path modifiedFile = Paths.get(confFile + ".mod");

        try
        {
            //Init gossiper local state
            Gossiper.instance.maybeInitializeLocalState(0);

            //replace file (cassandra-rackdc.properties.mod file has prefer_local=true)
            Files.copy(effectiveFile, backupFile);
            Files.copy(modifiedFile, effectiveFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

            //Init snitch
            final GossipingPropertyFileSnitch snitch = new GossipingPropertyFileSnitch(/*refreshPeriodInSeconds*/1);

            //prefer_Local is set to true, so local_address should be broadcast in gossip
            assertEquals("127.0.0.2", getInternalIp().value);

        }
        finally
        {
            Files.copy(backupFile, effectiveFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            Files.delete(backupFile);
        }
    }

    @Test
    public void testPreferLocalWithoutSpecifyingLocalAddress() throws Exception
    {
        //Define configuration files
        String confFile = FBUtilities.resourceToFile(SnitchProperties.RACKDC_PROPERTY_FILENAME);
        final Path effectiveFile = Paths.get(confFile);
        final Path backupFile = Paths.get(confFile + ".bak");
        final Path modifiedFile = Paths.get(confFile + ".mod2");

        try
        {
            //Init gossiper local state
            Gossiper.instance.maybeInitializeLocalState(0);

            //replace file (cassandra-rackdc.properties.mod file has prefer_local=true)
            Files.copy(effectiveFile, backupFile);
            Files.copy(modifiedFile, effectiveFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

            //Init snitch
            final GossipingPropertyFileSnitch snitch = new GossipingPropertyFileSnitch(/*refreshPeriodInSeconds*/1);

            //local_address=null, so listen_address should be broadcast in gossip
            assertEquals("127.0.0.1", getInternalIp().value);

        }
        finally
        {
            Files.copy(backupFile, effectiveFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            Files.delete(backupFile);
        }
    }

    private VersionedValue getInternalIp()
    {
        Gossiper gossiper = Gossiper.instance;
        final EndpointState es = gossiper.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddress());
        return es.getApplicationState(ApplicationState.INTERNAL_IP);
    }
}
