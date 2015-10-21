/**
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
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ResourceWatcher;
import org.apache.cassandra.utils.WrappedRunnable;


public class GossipingPropertyFileSnitch extends AbstractNetworkTopologySnitch// implements IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(GossipingPropertyFileSnitch.class);

    private PropertyFileSnitch psnitch;

    private volatile String myDC;
    private volatile String myRack;
    private volatile boolean preferLocal;
    private volatile String localAddress;

    private AtomicReference<ReconnectableSnitchHelper> snitchHelperReference;
    private volatile boolean gossipStarted;

    private Map<InetAddress, Map<String, String>> savedEndpoints;
    private static final String DEFAULT_DC = "UNKNOWN_DC";
    private static final String DEFAULT_RACK = "UNKNOWN_RACK";

    private static final int DEFAULT_REFRESH_PERIOD_IN_SECONDS = 60;
    
    public GossipingPropertyFileSnitch() throws ConfigurationException
    {
        this(DEFAULT_REFRESH_PERIOD_IN_SECONDS);
    }

    public GossipingPropertyFileSnitch(int refreshPeriodInSeconds) throws ConfigurationException
    {
        snitchHelperReference = new AtomicReference<ReconnectableSnitchHelper>();

        reloadConfiguration(false);

        try
        {
            psnitch = new PropertyFileSnitch();
            logger.info("Loaded {} for compatibility", PropertyFileSnitch.SNITCH_PROPERTIES_FILENAME);
        }
        catch (ConfigurationException e)
        {
            logger.info("Unable to load {}; compatibility mode disabled", PropertyFileSnitch.SNITCH_PROPERTIES_FILENAME);
        }

        try
        {
            FBUtilities.resourceToFile(SnitchProperties.RACKDC_PROPERTY_FILENAME);
            Runnable runnable = new WrappedRunnable()
            {
                protected void runMayThrow() throws ConfigurationException
                {
                    reloadConfiguration(true);
                }
            };
            ResourceWatcher.watch(SnitchProperties.RACKDC_PROPERTY_FILENAME, runnable, refreshPeriodInSeconds * 1000);
        }
        catch (ConfigurationException ex)
        {
            logger.error("{} found, but does not look like a plain file. Will not watch it for changes", SnitchProperties.RACKDC_PROPERTY_FILENAME);
        }
    }

    /**
     * Return the data center for which an endpoint resides in
     *
     * @param endpoint the endpoint to process
     * @return string of data center
     */
    public String getDatacenter(InetAddress endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
            return myDC;

        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (epState == null || epState.getApplicationState(ApplicationState.DC) == null)
        {
            if (psnitch == null)
            {
                if (savedEndpoints == null)
                    savedEndpoints = SystemKeyspace.loadDcRackInfo();
                if (savedEndpoints.containsKey(endpoint))
                    return savedEndpoints.get(endpoint).get("data_center");
                return DEFAULT_DC;
            }
            else
                return psnitch.getDatacenter(endpoint);
        }
        return epState.getApplicationState(ApplicationState.DC).value;
    }

    /**
     * Return the rack for which an endpoint resides in
     *
     * @param endpoint the endpoint to process
     * @return string of rack
     */
    public String getRack(InetAddress endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
            return myRack;

        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (epState == null || epState.getApplicationState(ApplicationState.RACK) == null)
        {
            if (psnitch == null)
            {
                if (savedEndpoints == null)
                    savedEndpoints = SystemKeyspace.loadDcRackInfo();
                if (savedEndpoints.containsKey(endpoint))
                    return savedEndpoints.get(endpoint).get("rack");
                return DEFAULT_RACK;
            }
            else
                return psnitch.getRack(endpoint);
        }
        return epState.getApplicationState(ApplicationState.RACK).value;
    }

    public void gossiperStarting()
    {
        super.gossiperStarting();

        reloadGossiperState();

        gossipStarted = true;
    }

    private void reloadConfiguration(boolean isUpdate) throws ConfigurationException
    {
        final SnitchProperties properties = new SnitchProperties();

        String newDc = properties.get("dc", null);
        String newRack = properties.get("rack", null);
        if (newDc == null || newRack == null)
            throw new ConfigurationException("DC or rack not found in snitch properties, check your configuration in: " + SnitchProperties.RACKDC_PROPERTY_FILENAME);

        newDc = newDc.trim();
        newRack = newRack.trim();
        final boolean newPreferLocal = Boolean.parseBoolean(properties.get("prefer_local", "false"));
        String newLocalAddress = getAndValidateLocalAddress(properties);

        if (!newDc.equals(myDC) || !newRack.equals(myRack) || (preferLocal != newPreferLocal) ||
            (newLocalAddress != null && !newLocalAddress.equals(localAddress)))
        {
            myDC = newDc;
            myRack = newRack;
            preferLocal = newPreferLocal;
            localAddress = newLocalAddress;

            reloadGossiperState();

            if (StorageService.instance != null)
            {
                if (isUpdate)
                    StorageService.instance.updateTopology(FBUtilities.getBroadcastAddress());
                else
                    StorageService.instance.getTokenMetadata().invalidateCachedRings();
            }

            if (gossipStarted)
                StorageService.instance.gossipSnitchInfo();
        }
    }

    private String getAndValidateLocalAddress(SnitchProperties properties) throws ConfigurationException
    {
        String localAddressStr = properties.get("local_address", null);
        if (localAddressStr != null)
        {
            try
            {
                InetAddress address = InetAddress.getByName(localAddressStr);
                if (address.isAnyLocalAddress())
                    throw new ConfigurationException("GossipingPropertyFileSnitch property local_address cannot be set " +
                                                     "to a wildcard address (" + localAddressStr + ").");
            }
            catch (UnknownHostException e)
            {
                throw new ConfigurationException("Unknown host for local_address property: " +
                                                 SnitchProperties.RACKDC_PROPERTY_FILENAME, e);
            }
        }
        return localAddressStr;
    }

    private void reloadGossiperState()
    {
        maybeUpdateGossipApplicationState();
        if (Gossiper.instance != null)
        {
            ReconnectableSnitchHelper pendingHelper = new ReconnectableSnitchHelper(this, myDC, preferLocal);
            Gossiper.instance.register(pendingHelper);
            
            pendingHelper = snitchHelperReference.getAndSet(pendingHelper);
            if (pendingHelper != null)
                Gossiper.instance.unregister(pendingHelper);
        }
        // else this will eventually rerun at gossiperStarting()
    }

    /**
     * be careful about just blindly updating ApplicationState.INTERNAL_IP everytime we reload the rackdc file,
     * as that can cause connections to get unnecessarily reset (via IESCS.onChange()).
     */
    private void maybeUpdateGossipApplicationState()
    {
        if (!preferLocal)
            return;

        if (localAddress == null && FBUtilities.getLocalAddress().isAnyLocalAddress()) {
            logger.warn("GossipingPropertyFileSnitch option prefer_local was set to true, but local_address snitch" +
                        "option was not specified and listen_address is {}. Please set snitch option local_address" +
                        " to define the preferred local IP if you're listening on multiple interfaces.",
                        FBUtilities.getLocalAddress());
            return;
        }

        String internalIp = localAddress != null ? localAddress : FBUtilities.getLocalAddress().getHostAddress();

        final EndpointState es = Gossiper.instance.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddress());
        if (es == null)
            return;
        final VersionedValue vv = es.getApplicationState(ApplicationState.INTERNAL_IP);
        if ((vv != null && !vv.value.equals(internalIp)) || vv == null)
        {
            logger.debug("Setting gossip {} field to: {}", ApplicationState.INTERNAL_IP, internalIp);
            Gossiper.instance.addLocalApplicationState(ApplicationState.INTERNAL_IP,
                                                       StorageService.instance.valueFactory.internalIP(internalIp));
        }
    }

}
