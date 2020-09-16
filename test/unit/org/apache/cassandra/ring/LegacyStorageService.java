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

package org.apache.cassandra.ring;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Multimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.service.StorageService.extractExpireTime;
import static org.apache.cassandra.service.StorageService.splitValue;

public class LegacyStorageService implements StorageServiceAdapter
{
    final static String KEYSPACE_NAME = "KEYSPACE1";

    private static final Logger logger = LoggerFactory.getLogger(LegacyStorageService.class);

    private final TokenMetadata tokenMetadata = new TokenMetadata();
    private final Function<InetAddressAndPort, NodeInfo> getNodeInfo;
    private final NetworkTopologyStrategy nts;

    private final boolean replacing = false;
    private final boolean isReplacingSameAddress = false;
    private final boolean localNodeIsMoving = false;

    public LegacyStorageService(Map<String, String> dcRfs, Function<InetAddressAndPort, NodeInfo> nodeInfoGetter)
    {
        this.getNodeInfo = nodeInfoGetter;
        IEndpointSnitch mockSnitch = getMockSnitch();
        /**
         * Make sure to unset the snitch during test cleanup as in {@link MultiDatacenterRingTest#afterClass()}
         */
        DatabaseDescriptor.setEndpointSnitch(mockSnitch);
        this.nts = new NetworkTopologyStrategy(KEYSPACE_NAME, tokenMetadata, mockSnitch, dcRfs);
    }

    public RingOverlay getRing()
    {
        return new RingOverlay()
        {
            public ReplicationGroup getReplicasForTokenWrite(Token token)
            {
                EndpointsForToken naturalReplicasForToken = nts.getNaturalReplicasForToken(token);
                return new ReplicationGroup(naturalReplicasForToken.endpoints().stream().map(e -> getNodeInfo.apply(e).id).collect(Collectors.toList()));
            }
        };
    }

    public void onJoin(InetAddressAndPort endpoint, EndpointState epState)
    {

    }

    public void beforeChange(InetAddressAndPort endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
    {

    }

    /** Copied from {@link org.apache.cassandra.service.StorageService#onChange(InetAddressAndPort, ApplicationState, VersionedValue)} **/

    /*
     * Handle the reception of a new particular ApplicationState for a particular endpoint. Note that the value of the
     * ApplicationState has not necessarily "changed" since the last known value, if we already received the same update
     * from somewhere else.
     *
     * onChange only ever sees one ApplicationState piece change at a time (even if many ApplicationState updates were
     * received at the same time), so we perform a kind of state machine here. We are concerned with two events: knowing
     * the token associated with an endpoint, and knowing its operation mode. Nodes can start in either bootstrap or
     * normal mode, and from bootstrap mode can change mode to normal. A node in bootstrap mode needs to have
     * pendingranges set in TokenMetadata; a node in normal mode should instead be part of the token ring.
     *
     * Normal progression of ApplicationState.STATUS values for a node should be like this:
     * STATUS_BOOTSTRAPPING,token
     *   if bootstrapping. stays this way until all files are received.
     * STATUS_NORMAL,token
     *   ready to serve reads and writes.
     * STATUS_LEAVING,token
     *   get ready to leave the cluster as part of a decommission
     * STATUS_LEFT,token
     *   set after decommission is completed.
     *
     * Other STATUS values that may be seen (possibly anywhere in the normal progression):
     * STATUS_MOVING,newtoken
     *   set if node is currently moving to a new token in the ring
     * REMOVING_TOKEN,deadtoken
     *   set if the node is dead and is being removed by its REMOVAL_COORDINATOR
     * REMOVED_TOKEN,deadtoken
     *   set if the node is dead and has been removed by its REMOVAL_COORDINATOR
     *
     * Note: Any time a node state changes from STATUS_NORMAL, it will not be visible to new nodes. So it follows that
     * you should never bootstrap a new node during a removenode, decommission or move.
     */
    public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
    {
        if (state == ApplicationState.STATUS || state == ApplicationState.STATUS_WITH_PORT)
        {
            String[] pieces = splitValue(value);
            assert (pieces.length > 0);

            String moveName = pieces[0];

            switch (moveName)
            {
                case VersionedValue.STATUS_BOOTSTRAPPING_REPLACE:
                    handleStateBootreplacing(endpoint, pieces);
                    break;
                case VersionedValue.STATUS_BOOTSTRAPPING:
                    handleStateBootstrap(endpoint);
                    break;
                case VersionedValue.STATUS_NORMAL:
                    handleStateNormal(endpoint, VersionedValue.STATUS_NORMAL);
                    break;
                case VersionedValue.SHUTDOWN:
                    handleStateNormal(endpoint, VersionedValue.SHUTDOWN);
                    break;
                case VersionedValue.REMOVING_TOKEN:
                case VersionedValue.REMOVED_TOKEN:
                    handleStateRemoving(endpoint, pieces);
                    break;
                case VersionedValue.STATUS_LEAVING:
                    handleStateLeaving(endpoint);
                    break;
                case VersionedValue.STATUS_LEFT:
                    handleStateLeft(endpoint, pieces);
                    break;
                case VersionedValue.STATUS_MOVING:
                    handleStateMoving(endpoint, pieces);
                    break;
            }
        }
        else
        {
            EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
            if (epState == null || Gossiper.instance.isDeadState(epState))
            {
                logger.debug("Ignoring state change for dead or unknown endpoint: {}", endpoint);
                return;
            }

            if (tokenMetadata.isMember(endpoint))
            {
                switch (state)
                {
                    case RELEASE_VERSION:
                        SystemKeyspace.updatePeerInfo(endpoint, "release_version", value.value);
                        break;
                    case DC:
                        updateTopology(endpoint);
                        SystemKeyspace.updatePeerInfo(endpoint, "data_center", value.value);
                        break;
                    case RACK:
                        updateTopology(endpoint);
                        SystemKeyspace.updatePeerInfo(endpoint, "rack", value.value);
                        break;
                }
            }
        }
    }

    public void onAlive(InetAddressAndPort endpoint, EndpointState state)
    {

    }

    public void onDead(InetAddressAndPort endpoint, EndpointState state)
    {

    }

    public void onRemove(InetAddressAndPort endpoint)
    {

    }

    public void onRestart(InetAddressAndPort endpoint, EndpointState state)
    {

    }

    /**
     * Handle node bootstrap
     *
     * @param endpoint bootstrapping node
     */
    private void handleStateBootstrap(InetAddressAndPort endpoint)
    {
        Collection<Token> tokens;
        // explicitly check for TOKENS, because a bootstrapping node might be bootstrapping in legacy mode; that is, not using vnodes and no token specified
        tokens = getTokensFor(endpoint);

        if (logger.isDebugEnabled())
            logger.debug("Node {} state bootstrapping, token {}", endpoint, tokens);

        // if this node is present in token metadata, either we have missed intermediate states
        // or the node had crashed. Print warning if needed, clear obsolete stuff and
        // continue.
        if (tokenMetadata.isMember(endpoint))
        {
            // If isLeaving is false, we have missed both LEAVING and LEFT. However, if
            // isLeaving is true, we have only missed LEFT. Waiting time between completing
            // leave operation and rebootstrapping is relatively short, so the latter is quite
            // common (not enough time for gossip to spread). Therefore we report only the
            // former in the log.
            if (!tokenMetadata.isLeaving(endpoint))
                logger.info("Node {} state jump to bootstrap", endpoint);
            tokenMetadata.removeEndpoint(endpoint);
        }

        tokenMetadata.addBootstrapTokens(tokens, endpoint);
        PendingRangeCalculatorService.instance.update();

        tokenMetadata.updateHostId(Gossiper.instance.getHostId(endpoint), endpoint);
    }

    private void handleStateBootreplacing(InetAddressAndPort newNode, String[] pieces)
    {
        InetAddressAndPort oldNode;
        try
        {
            oldNode = InetAddressAndPort.getByName(pieces[1]);
        }
        catch (Exception e)
        {
            logger.error("Node {} tried to replace malformed endpoint {}.", newNode, pieces[1], e);
            return;
        }

        if (FailureDetector.instance.isAlive(oldNode))
        {
            throw new RuntimeException(String.format("Node %s is trying to replace alive node %s.", newNode, oldNode));
        }

        Optional<InetAddressAndPort> replacingNode = tokenMetadata.getReplacingNode(newNode);
        if (replacingNode.isPresent() && !replacingNode.get().equals(oldNode))
        {
            throw new RuntimeException(String.format("Node %s is already replacing %s but is trying to replace %s.",
                                                     newNode, replacingNode.get(), oldNode));
        }

        Collection<Token> tokens = getTokensFor(newNode);

        if (logger.isDebugEnabled())
            logger.debug("Node {} is replacing {}, tokens {}", newNode, oldNode, tokens);

        tokenMetadata.addReplaceTokens(tokens, newNode, oldNode);
        PendingRangeCalculatorService.instance.update();

        tokenMetadata.updateHostId(Gossiper.instance.getHostId(newNode), newNode);
    }

    /**
     * Handle node move to normal state. That is, node is entering token ring and participating
     * in reads.
     *
     * @param endpoint node
     */
    private void handleStateNormal(final InetAddressAndPort endpoint, final String status)
    {
        Collection<Token> tokens = getTokensFor(endpoint);
        Set<InetAddressAndPort> endpointsToRemove = new HashSet<>();

        if (logger.isDebugEnabled())
            logger.debug("Node {} state {}, token {}", endpoint, status, tokens);

        if (tokenMetadata.isMember(endpoint))
            logger.info("Node {} state jump to {}", endpoint, status);

        if (tokens.isEmpty() && status.equals(VersionedValue.STATUS_NORMAL))
            logger.error("Node {} is in state normal but it has no tokens, state: {}",
                         endpoint,
                         Gossiper.instance.getEndpointStateForEndpoint(endpoint));

        Optional<InetAddressAndPort> replacingNode = tokenMetadata.getReplacingNode(endpoint);
        if (replacingNode.isPresent())
        {
            assert !endpoint.equals(replacingNode.get()) : "Pending replacement endpoint with same address is not supported";
            logger.info("Node {} will complete replacement of {} for tokens {}", endpoint, replacingNode.get(), tokens);
            if (FailureDetector.instance.isAlive(replacingNode.get()))
            {
                logger.error("Node {} cannot complete replacement of alive node {}.", endpoint, replacingNode.get());
                return;
            }
            endpointsToRemove.add(replacingNode.get());
        }

        Optional<InetAddressAndPort> replacementNode = tokenMetadata.getReplacementNode(endpoint);
        if (replacementNode.isPresent())
        {
            logger.warn("Node {} is currently being replaced by node {}.", endpoint, replacementNode.get());
        }

        // TODO updatePeerInfo(endpoint);
        // Order Matters, TM.updateHostID() should be called before TM.updateNormalToken(), (see CASSANDRA-4300).
        UUID hostId = getNodeInfo.apply(endpoint).id;
        InetAddressAndPort existing = tokenMetadata.getEndpointForHostId(hostId);
        if (replacing && isReplacingSameAddress && Gossiper.instance.getEndpointStateForEndpoint(DatabaseDescriptor.getReplaceAddress()) != null
            && (hostId.equals(getNodeInfo.apply(DatabaseDescriptor.getReplaceAddress()).id)))
            logger.warn("Not updating token metadata for {} because I am replacing it", endpoint);
        else
        {
            if (existing != null && !existing.equals(endpoint))
            {
                if (existing.equals(FBUtilities.getBroadcastAddressAndPort()))
                {
                    logger.warn("Not updating host ID {} for {} because it's mine", hostId, endpoint);
                    tokenMetadata.removeEndpoint(endpoint);
                    endpointsToRemove.add(endpoint);
                }
// FIXME               else if (Gossiper.instance.compareEndpointStartup(endpoint, existing) > 0)
//                {
//                    logger.warn("Host ID collision for {} between {} and {}; {} is the new owner", hostId, existing, endpoint, endpoint);
//                    tokenMetadata.removeEndpoint(existing);
//                    endpointsToRemove.add(existing);
//                    tokenMetadata.updateHostId(hostId, endpoint);
//                }
                else
                {
                    logger.warn("Host ID collision for {} between {} and {}; ignored {}", hostId, existing, endpoint, endpoint);
                    tokenMetadata.removeEndpoint(endpoint);
                    endpointsToRemove.add(endpoint);
                }
            }
            else
                tokenMetadata.updateHostId(hostId, endpoint);
        }

        // capture because updateNormalTokens clears moving and member status
        boolean isMember = tokenMetadata.isMember(endpoint);
        boolean isMoving = tokenMetadata.isMoving(endpoint);

        updateTokenMetadata(endpoint, tokens, endpointsToRemove);

        if (isMoving || localNodeIsMoving)
        {
            tokenMetadata.removeFromMoving(endpoint);
            //notifyMoved(endpoint);
        }
        else if (!isMember) // prior to this, the node was not a member
        {
            //notifyJoined(endpoint);
        }

        tokenMetadata.calculatePendingRanges(nts, KEYSPACE_NAME);
    }

    /**
     * Handle notification that a node being actively removed from the ring via 'removenode'
     *
     * @param endpoint node
     * @param pieces either REMOVED_TOKEN (node is gone) or REMOVING_TOKEN (replicas need to be restored)
     */
    private void handleStateRemoving(InetAddressAndPort endpoint, String[] pieces)
    {
        assert (pieces.length > 0);

        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
        {
            logger.info("Received removenode gossip about myself. Is this node rejoining after an explicit removenode?");
            try
            {
                StorageService.instance.drain();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            return;
        }
        if (tokenMetadata.isMember(endpoint))
        {
            String state = pieces[0];
            Collection<Token> removeTokens = tokenMetadata.getTokens(endpoint);

            if (VersionedValue.REMOVED_TOKEN.equals(state))
            {
                excise(removeTokens, endpoint, extractExpireTime(pieces));
            }
            else if (VersionedValue.REMOVING_TOKEN.equals(state))
            {
                ensureUpToDateTokenMetadata(state, endpoint);

                if (logger.isDebugEnabled())
                    logger.debug("Tokens {} removed manually (endpoint was {})", removeTokens, endpoint);

                // Note that the endpoint is being removed
                tokenMetadata.addLeavingEndpoint(endpoint);
                PendingRangeCalculatorService.instance.update();

                // find the endpoint coordinating this removal that we need to notify when we're done
                String[] coordinator = splitValue(Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.REMOVAL_COORDINATOR));
                UUID hostId = UUID.fromString(coordinator[1]);
                // grab any data we are now responsible for and notify responsible node
                StorageService.instance.restoreReplicaCount(endpoint, tokenMetadata.getEndpointForHostId(hostId));
            }
        }
        else // now that the gossiper has told us about this nonexistent member, notify the gossiper to remove it
        {
            if (VersionedValue.REMOVED_TOKEN.equals(pieces[0]))
                addExpireTimeIfFound(endpoint, extractExpireTime(pieces));
            removeEndpoint(endpoint);
        }
    }

    private void excise(Collection<Token> tokens, InetAddressAndPort endpoint, long expireTime)
    {
        addExpireTimeIfFound(endpoint, expireTime);
        excise(tokens, endpoint);
    }

    private void excise(Collection<Token> tokens, InetAddressAndPort endpoint)
    {
        logger.info("Removing tokens {} for {}", tokens, endpoint);

        UUID hostId = tokenMetadata.getHostId(endpoint);
        if (hostId != null && tokenMetadata.isMember(endpoint))
        {
            // enough time for writes to expire and MessagingService timeout reporter callback to fire, which is where
            // hints are mostly written from - using getMinRpcTimeout() / 2 for the interval.
            long delay = DatabaseDescriptor.getMinRpcTimeout(MILLISECONDS) + DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS);
            ScheduledExecutors.optionalTasks.schedule(() -> HintsService.instance.excise(hostId), delay, MILLISECONDS);
        }

        removeEndpoint(endpoint);
        tokenMetadata.removeEndpoint(endpoint);
        if (!tokens.isEmpty())
            tokenMetadata.removeBootstrapTokens(tokens);
        //notifyLeft(endpoint);
        PendingRangeCalculatorService.instance.update();
    }

    /**
     * Handle node preparing to leave the ring
     *
     * @param endpoint node
     */
    private void handleStateLeaving(InetAddressAndPort endpoint)
    {
        // If the node is previously unknown or tokens do not match, update tokenmetadata to
        // have this node as 'normal' (it must have been using this token before the
        // leave). This way we'll get pending ranges right.

        ensureUpToDateTokenMetadata(VersionedValue.STATUS_LEAVING, endpoint);

        // at this point the endpoint is certainly a member with this token, so let's proceed
        // normally
        tokenMetadata.addLeavingEndpoint(endpoint);
        PendingRangeCalculatorService.instance.update();
    }

    /**
     * Handle node leaving the ring. This will happen when a node is decommissioned
     *
     * @param endpoint If reason for leaving is decommission, endpoint is the leaving node.
     * @param pieces STATE_LEFT,token
     */
    private void handleStateLeft(InetAddressAndPort endpoint, String[] pieces)
    {
        assert pieces.length >= 2;
        Collection<Token> tokens = getTokensFor(endpoint);

        if (logger.isDebugEnabled())
            logger.debug("Node {} state left, tokens {}", endpoint, tokens);

        excise(tokens, endpoint, extractExpireTime(pieces));
    }

    /**
     * Handle node moving inside the ring.
     *
     * @param endpoint moving endpoint address
     * @param pieces STATE_MOVING, token
     */
    private void handleStateMoving(InetAddressAndPort endpoint, String[] pieces)
    {
        ensureUpToDateTokenMetadata(VersionedValue.STATUS_MOVING, endpoint);

        assert pieces.length >= 2;
        Token token = tokenMetadata.partitioner.getTokenFactory().fromString(pieces[1]);

        if (logger.isDebugEnabled())
            logger.debug("Node {} state moving, new token {}", endpoint, token);

        tokenMetadata.addMovingEndpoint(token, endpoint);

        PendingRangeCalculatorService.instance.update();
    }

    public void updateTopology(InetAddressAndPort endpoint)
    {
        if (getTokenMetadata().isMember(endpoint))
        {
            getTokenMetadata().updateTopology(endpoint);
        }
    }

    private TokenMetadata getTokenMetadata()
    {
        return this.tokenMetadata;
    }

    private void updateTokenMetadata(InetAddressAndPort endpoint, Iterable<Token> tokens)
    {
        updateTokenMetadata(endpoint, tokens, new HashSet<>());
    }

    private void updateTokenMetadata(InetAddressAndPort endpoint, Iterable<Token> tokens, Set<InetAddressAndPort> endpointsToRemove)
    {
        Set<Token> tokensToUpdateInMetadata = new HashSet<>();
        Set<Token> tokensToUpdateInSystemKeyspace = new HashSet<>();

        for (final Token token : tokens)
        {
            // we don't want to update if this node is responsible for the token and it has a later startup time than endpoint.
            InetAddressAndPort currentOwner = tokenMetadata.getEndpoint(token);
            if (currentOwner == null)
            {
                logger.debug("New node {} at token {}", endpoint, token);
                tokensToUpdateInMetadata.add(token);
                tokensToUpdateInSystemKeyspace.add(token);
            }
            else if (endpoint.equals(currentOwner))
            {
                // set state back to normal, since the node may have tried to leave, but failed and is now back up
                tokensToUpdateInMetadata.add(token);
                tokensToUpdateInSystemKeyspace.add(token);
            }
            else if (Gossiper.instance.compareEndpointStartup(endpoint, currentOwner) > 0)
            {
                tokensToUpdateInMetadata.add(token);
                tokensToUpdateInSystemKeyspace.add(token);

                // currentOwner is no longer current, endpoint is.  Keep track of these moves, because when
                // a host no longer has any tokens, we'll want to remove it.
                Multimap<InetAddressAndPort, Token> epToTokenCopy = getTokenMetadata().getEndpointToTokenMapForReading();
                epToTokenCopy.get(currentOwner).remove(token);
                if (epToTokenCopy.get(currentOwner).isEmpty())
                    endpointsToRemove.add(currentOwner);

                logger.info("Nodes {} and {} have the same token {}. {} is the new owner", endpoint, currentOwner, token, endpoint);
            }
            else
            {
                logger.info("Nodes () and {} have the same token {}.  Ignoring {}", endpoint, currentOwner, token, endpoint);
            }
        }

        tokenMetadata.updateNormalTokens(tokensToUpdateInMetadata, endpoint);
        for (InetAddressAndPort ep : endpointsToRemove)
        {
            removeEndpoint(ep);
            if (replacing && ep.equals(DatabaseDescriptor.getReplaceAddress()))
                Gossiper.instance.replacementQuarantine(ep); // quarantine locally longer than normally; see CASSANDRA-8260
        }

        //if (!tokensToUpdateInSystemKeyspace.isEmpty())
        //    SystemKeyspace.updateTokens(endpoint, tokensToUpdateInSystemKeyspace);
    }

    private void ensureUpToDateTokenMetadata(String status, InetAddressAndPort endpoint)
    {
        Set<Token> tokens = new TreeSet<>(getTokensFor(endpoint));

        if (logger.isDebugEnabled())
            logger.debug("Node {} state {}, tokens {}", endpoint, status, tokens);

        // If the node is previously unknown or tokens do not match, update tokenmetadata to
        // have this node as 'normal' (it must have been using this token before the
        // leave). This way we'll get pending ranges right.
        if (!tokenMetadata.isMember(endpoint))
        {
            logger.info("Node {} state jump to {}", endpoint, status);
            updateTokenMetadata(endpoint, tokens);
        }
        else if (!tokens.equals(new TreeSet<>(tokenMetadata.getTokens(endpoint))))
        {
            logger.warn("Node {} '{}' token mismatch. Long network partition?", endpoint, status);
            updateTokenMetadata(endpoint, tokens);
        }
    }

    /** unlike excise we just need this endpoint gone without going through any notifications **/
    private void removeEndpoint(InetAddressAndPort endpoint)
    {
        //TODO fixme
        //Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.removeEndpoint(endpoint));
        //SystemKeyspace.removeEndpoint(endpoint);
    }

    private boolean isStatus(InetAddressAndPort endpoint, String status)
    {
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        return state != null && state.getStatus().equals(status);
    }

    protected void addExpireTimeIfFound(InetAddressAndPort endpoint, long expireTime)
    {
        if (expireTime != 0L)
        {
            Gossiper.instance.addExpireTimeForEndpoint(endpoint, expireTime);
        }
    }

    private Collection<Token> getTokensFor(InetAddressAndPort endpoint)
    {
        return getNodeInfo.apply(endpoint).tokens;
    }

    private IEndpointSnitch getMockSnitch()
    {
        return new AbstractEndpointSnitch()
        {
            public String getRack(InetAddressAndPort endpoint)
            {
                return getNodeInfo.apply(endpoint).rack;
            }

            public String getDatacenter(InetAddressAndPort endpoint)
            {
                return getNodeInfo.apply(endpoint).dc;
            }

            public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C addresses)
            {
                return addresses;
            }

            public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
            {
                return 0;
            }
        };
    }
}
