/*
 Copyright (C) 2018 Electronic Arts Inc.  All rights reserved.

 Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions
 are met:

 1.  Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.
 2.  Redistributions in binary form must reproduce the above copyright
     notice, this list of conditions and the following disclaimer in the
     documentation and/or other materials provided with the distribution.
 3.  Neither the name of Electronic Arts, Inc. ("EA") nor the names of
     its contributors may be used to endorse or promote products derived
     from this software without specific prior written permission.

 THIS SOFTWARE IS PROVIDED BY ELECTRONIC ARTS AND ITS CONTRIBUTORS "AS IS" AND ANY
 EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 DISCLAIMED. IN NO EVENT SHALL ELECTRONIC ARTS OR ITS CONTRIBUTORS BE LIABLE FOR ANY
 DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package cloud.orbit.actors.cluster.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cloud.orbit.actors.NodeState;
import cloud.orbit.actors.NodeType;
import cloud.orbit.actors.cluster.ClusterNodeView;
import cloud.orbit.actors.cluster.ClusterView;
import cloud.orbit.actors.cluster.NodeAddress;
import cloud.orbit.actors.cluster.RedisClusterConfig;
import cloud.orbit.actors.cluster.heartbeat.RedisClusterHeartBeat;
import cloud.orbit.actors.cluster.heartbeat.RedisClusterNodeView;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toMap;

/**
 * A <code>RedisClusterTracker</code> is responsible for tracking the state of the cluster, as perceived by the local
 * node, as updated by <code>RedisClusterHeartBeat</code>s received over Redis.
 */
public class RedisClusterTracker
{
    private static Logger logger = LoggerFactory.getLogger(RedisClusterTracker.class);

    private final RedisClusterConfig config;
    private final NodeAddress localAddress;
    private final Set<String> hostableInterfaces;

    private String nodeName; // human-friendly node name supplied by the application layer via config, such as "hostname:port"
    private volatile NodeType nodeType; // note: only valid transition for this field is CLIENT -> HOST.
    private volatile NodeState nodeState; // note: valid transitions are RUNNING -> STOPPING -> STOPPED and RUNNING -> PRESUMED_DEAD

    private volatile int sequenceNumber = 0;

    // field contents updated by heartbeats
    // note: contains a mapping for `localAddress` -> our own heartbeats
    private final ConcurrentMap<NodeAddress, RedisClusterNodeTracker> nodeTrackers = new ConcurrentHashMap<>();


    public RedisClusterTracker(final RedisClusterConfig config, final NodeAddress localAddress, final Set<String> hostableInterfaces)
    {
        this.config = config;
        this.localAddress = localAddress;
        this.hostableInterfaces = hostableInterfaces;
    }

    public RedisClusterHeartBeat createHeartBeat () {
        ++sequenceNumber;

        final Map<NodeAddress, RedisClusterNodeView> snapshotNodeViews = nodeTrackers.values().stream()
                .map(RedisClusterNodeTracker::createHeartBeatClusterNodeView)
                .collect(toMap(RedisClusterNodeView::getNodeAddress, Function.identity()));

        return new RedisClusterHeartBeat(
                this.localAddress,
                this.nodeName,
                this.nodeType,
                this.nodeState,
                this.sequenceNumber,
                this.hostableInterfaces,
                snapshotNodeViews
        );
    }

    public boolean receiveHeartBeat(final RedisClusterHeartBeat heartBeat)
    {
        final RedisClusterNodeTracker tracker = this.nodeTrackers.computeIfAbsent(heartBeat.getNodeAddress(), RedisClusterNodeTracker::new);
        return tracker.receiveHeartBeat(heartBeat);
    }

    public ClusterView createClusterView()
    {
        final SortedMap<NodeAddress, ClusterNodeView> sortedNodes = new TreeMap<>();

        nodeTrackers.values().stream()
                .map(RedisClusterNodeTracker::createClusterNodeView)
                .forEach(view -> sortedNodes.put(view.getNodeAddress(), view));

        return new ClusterView(sortedNodes);
    }

    public Set<NodeAddress> scanForDeadNodes ()
    {
        final long foreignNodeDeathTimeout = config.getForeignNodeDeathTimeoutMillis();
        final long localNodeDeathTimeout = config.getLocalNodeDeathTimeoutMillis();

        Set<NodeAddress> deadNodes = null;

        final long now = System.currentTimeMillis();
        for ( RedisClusterNodeTracker tracker : this.nodeTrackers.values() )
        {
            final long timeout = Objects.equals(tracker.getNodeAddress(), this.localAddress) ? localNodeDeathTimeout : foreignNodeDeathTimeout;
            final long timeoutTimestamp = tracker.getLastHeartBeatLocalTimestamp() + timeout;
            if ( now > timeoutTimestamp && tracker.getNodeState() == NodeState.RUNNING ) {
                tracker.setNodeState(NodeState.PRESUMED_DEAD);

                if ( deadNodes == null ) deadNodes = new HashSet<>();
                deadNodes.add(tracker.getNodeAddress());
            }
        }

        return deadNodes == null ? emptySet() : deadNodes;
    }

    public boolean cullLongDeadNodes ()
    {
        final long cullingTimeout = config.getDeadNodeCullingDelayMillis();
        final long now = System.currentTimeMillis();
        final long cullingThreshold = now - cullingTimeout;

        return this.nodeTrackers.values().removeIf(tracker -> tracker.getLastHeartBeatLocalTimestamp() < cullingThreshold);
    }

    /** Note: only works after calling scanForDeadNodes 'recently' (timeout-based) */
    public boolean isThisNodeDead()
    {
        return isThisNodeInState(NodeState.PRESUMED_DEAD);
    }

    public boolean isThisNodeInState(final NodeState nodeState)
    {
        final RedisClusterNodeTracker selfTracker = nodeTrackers.get(this.localAddress);
        return selfTracker != null && selfTracker.getNodeState() == nodeState;
    }

    public boolean isLocalNodeInCluster()
    {
        // Two conditions:
        // - we have heartbeats from 'enough' nodes (enough = config value)
        // - we take the set of all living nodes listed by all systems we have heartbeats from, then make sure that we
        //   have a heartbeat from ALL of those nodes that agree that we are in the cluster...
        return haveEnoughLivingNodesForAgreement() && clusterAgreesThatLocalIs(NodeState.RUNNING, null);
    }

    private boolean haveEnoughLivingNodesForAgreement () {
        final long livingNodeCount = nodeTrackers.values().stream()
                .filter(tracker -> tracker.getNodeState() == NodeState.RUNNING)
                .count();

        return livingNodeCount >= config.getMinNodesInCluster();
    }


    private boolean clusterAgreesThatLocalIs ( final NodeState requiredNodeState, final NodeType requiredNodeType ) {
        // Note: the same 'rumouredNodeAddress' will occur repeatedly -- once in each nodeTracker.
        // An alternative approach here would be to extract all rumouredNodeAddresses into a Set<NodeAddress>, then
        // run through and check each of those once instead of many times. However, that requires allocating a HashSet,
        // so I assume that this will be faster.
        for ( final RedisClusterNodeTracker tracker : nodeTrackers.values() ) {
            if ( tracker.getNodeState() != NodeState.RUNNING ) {
                continue;
            }

            for ( final RedisClusterNodeView view : tracker.getNodeViews().values() ) {
                // Ignore any rumoured nodes that aren't believed to be RUNNING.
                if ( view.getNodeState() != NodeState.RUNNING ) {
                    continue;
                }

                final NodeAddress rumouredNodeAddress = view.getNodeAddress();
                final RedisClusterNodeTracker rumouredNodeTracker = nodeTrackers.get(rumouredNodeAddress);

                // Some node (B) that we (A) have heard from knows of a different node (C) that we (A) haven't heard from.
                if ( rumouredNodeTracker == null )
                {
                    logger.debug("clusterAgreesThatLocalIs {} {} -> FALSE because we have no record of {}", requiredNodeState, requiredNodeType, rumouredNodeAddress);
                    return false;
                }

                // How does node C see us? If C doesn't report seeing us yet, or doesn't see us with the required
                // nodeState / nodeType, then we aren't officially in that state yet.
                final RedisClusterNodeView viewOfLocal = rumouredNodeTracker.getNodeViews().get(localAddress);
                if ( viewOfLocal == null )
                {
                    logger.debug("clusterAgreesThatLocalIs {} {} -> FALSE because {} does not see us", requiredNodeState, requiredNodeType, rumouredNodeAddress);
                    return false;
                }

                if ( requiredNodeState != null && viewOfLocal.getNodeState() != requiredNodeState )
                {
                    logger.debug("clusterAgreesThatLocalIs {} {} -> FALSE because {} sees us as {}", requiredNodeState, requiredNodeType, rumouredNodeAddress, viewOfLocal.getNodeState());
                    return false;
                }

                if ( requiredNodeType != null && viewOfLocal.getNodeType() != requiredNodeType )
                {
                    logger.debug("clusterAgreesThatLocalIs {} {} -> FALSE because {} sees us as {}", requiredNodeState, requiredNodeType, rumouredNodeAddress, viewOfLocal.getNodeType());
                    return false;
                }
            }
        }

        return true;
    }

    public String getNodeName()
    {
        return nodeName;
    }

    public void setNodeName(final String nodeName)
    {
        this.nodeName = nodeName;
    }

    public NodeType getNodeType()
    {
        return nodeType;
    }

    public void setNodeType(final NodeType nodeType)
    {
        this.nodeType = nodeType;
    }

    public NodeState getNodeState()
    {
        return nodeState;
    }

    public void setNodeState(final NodeState nodeState)
    {
        this.nodeState = nodeState;
    }

    public ConcurrentMap<NodeAddress, RedisClusterNodeTracker> getNodeTrackers()
    {
        return nodeTrackers;
    }
}
