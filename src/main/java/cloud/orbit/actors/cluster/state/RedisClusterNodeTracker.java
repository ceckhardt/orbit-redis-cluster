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

import cloud.orbit.actors.NodeState;
import cloud.orbit.actors.NodeType;
import cloud.orbit.actors.cluster.ClusterNodeView;
import cloud.orbit.actors.cluster.NodeAddress;
import cloud.orbit.actors.cluster.heartbeat.RedisClusterHeartBeat;
import cloud.orbit.actors.cluster.heartbeat.RedisClusterNodeView;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A <code>RedisClusterNodeTracker</code> is a mutable tracker that represents Node A's view of Node B and of Node B's
 * view of the cluster.
 */
public class RedisClusterNodeTracker
{
    private final NodeAddress nodeAddress;

    private String nodeName;
    private NodeType nodeType;
    private NodeState nodeState;
    private String placementGroup;
    private Set<String> hostableActorInterfaces;


    private long lastHeartBeatLocalTimestamp = -1;
    private int lastHeartBeatSequenceNumber = -1;


    private int missedSequenceNumberCount = 0;
    private int longestMissedSequenceNumberStreak = 0;

    private Map<NodeAddress, RedisClusterNodeView> nodeViews;


    public RedisClusterNodeTracker(final NodeAddress nodeAddress)
    {
        this.nodeAddress = nodeAddress;
    }

    public synchronized boolean receiveHeartBeat(final RedisClusterHeartBeat heartBeat) {
        // Discard any out-of-order heartbeats
        if ( heartBeat.getSequenceNumber() <= this.lastHeartBeatSequenceNumber ) {
            return false;
        }

        this.lastHeartBeatLocalTimestamp = System.currentTimeMillis();

        if ( this.lastHeartBeatSequenceNumber >= 0 )
        {
            int expectedSequenceNumber = this.lastHeartBeatSequenceNumber + 1;
            int missedSequenceNumbers = heartBeat.getSequenceNumber() - expectedSequenceNumber;
            this.missedSequenceNumberCount += missedSequenceNumbers;
            this.longestMissedSequenceNumberStreak = Math.max(this.longestMissedSequenceNumberStreak, missedSequenceNumbers);
        }

        this.lastHeartBeatSequenceNumber = heartBeat.getSequenceNumber();

        // The view is updated if (a) any node transitions CLIENT -> HOST, (b) any note transitions state, (c) any node
        // changes its advertised set of hostable actors. The cluster view is not sensitive to other node's cluster views.
        final boolean isViewUpdated = ! Objects.equals(this.nodeType, heartBeat.getNodeType())
                || ! Objects.equals(this.nodeState, heartBeat.getNodeState())
                || ! Objects.equals(this.placementGroup, heartBeat.getPlacementGroup())
                || ! Objects.equals(this.hostableActorInterfaces, heartBeat.getHostableInterfaces());

        this.nodeName = heartBeat.getNodeName();
        this.nodeType = heartBeat.getNodeType();
        this.nodeState = heartBeat.getNodeState();
        this.placementGroup = heartBeat.getPlacementGroup();
        this.hostableActorInterfaces = heartBeat.getHostableInterfaces();
        this.nodeViews = heartBeat.getNodeViews();

        return isViewUpdated;
    }

    public RedisClusterNodeView createHeartBeatClusterNodeView()
    {
        return new RedisClusterNodeView(
                this.nodeAddress,
                this.nodeType,
                this.nodeState,
                this.lastHeartBeatLocalTimestamp,
                this.lastHeartBeatSequenceNumber,
                this.missedSequenceNumberCount,
                this.longestMissedSequenceNumberStreak
        );
    }

    public ClusterNodeView createClusterNodeView()
    {
        return new ClusterNodeView(
                this.nodeAddress,
                this.nodeName,
                this.nodeType,
                this.nodeState,
                this.placementGroup,
                this.hostableActorInterfaces
        );
    }

    public NodeAddress getNodeAddress()
    {
        return nodeAddress;
    }

    public String getNodeName()
    {
        return nodeName;
    }

    public NodeType getNodeType()
    {
        return nodeType;
    }

    public NodeState getNodeState()
    {
        return nodeState;
    }

    public void setNodeState(final NodeState nodeState)
    {
        this.nodeState = nodeState;
    }

    public Set<String> getHostableActorInterfaces()
    {
        return hostableActorInterfaces;
    }

    public long getLastHeartBeatLocalTimestamp()
    {
        return lastHeartBeatLocalTimestamp;
    }

    public int getLastHeartBeatSequenceNumber()
    {
        return lastHeartBeatSequenceNumber;
    }

    public int getMissedSequenceNumberCount()
    {
        return missedSequenceNumberCount;
    }

    public int getLongestMissedSequenceNumberStreak()
    {
        return longestMissedSequenceNumberStreak;
    }

    public Map<NodeAddress, RedisClusterNodeView> getNodeViews()
    {
        return nodeViews;
    }
}
