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

package cloud.orbit.actors.cluster.heartbeat;

import cloud.orbit.actors.NodeState;
import cloud.orbit.actors.NodeType;
import cloud.orbit.actors.cluster.NodeAddress;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * A <code>RedisClusterHeartBeat</code> is the data type that gets sent over Redis Pub/Sub to allow nodes to share their
 * local view of the cluster and (eventually) reach consensus about which nodes are in the cluster and in which mode.
 *
 * Received instances of this class are used to update a node's local <code>RedisClusterTracker</code>, which can also
 * produce new instances of this class to send back out to the cluster.
 */
public class RedisClusterHeartBeat implements Serializable
{
    private final NodeAddress nodeAddress;
    private final String nodeName; // human-friendly name, such as "hostname:port"

    private final NodeType nodeType;
    private final NodeState nodeState;

    private final int sequenceNumber;

    private final String placementGroup;
    private final Set<String> hostableInterfaces;

    private final Map<NodeAddress, RedisClusterNodeView> nodeViews;

    public RedisClusterHeartBeat(
            final NodeAddress nodeAddress,
            final String nodeName,
            final NodeType nodeType,
            final NodeState nodeState,
            final int sequenceNumber,
            final String placementGroup,
            final Set<String> hostableInterfaces,
            final Map<NodeAddress, RedisClusterNodeView> nodeViews)
    {
        this.nodeAddress = nodeAddress;
        this.nodeName = nodeName;

        this.nodeType = nodeType;
        this.nodeState = nodeState;

        this.sequenceNumber = sequenceNumber;

        this.placementGroup = placementGroup;
        this.hostableInterfaces = hostableInterfaces;

        this.nodeViews = nodeViews;
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

    public int getSequenceNumber()
    {
        return sequenceNumber;
    }

    public String getPlacementGroup()
    {
        return placementGroup;
    }

    public Set<String> getHostableInterfaces()
    {
        return hostableInterfaces;
    }

    public Map<NodeAddress, RedisClusterNodeView> getNodeViews()
    {
        return nodeViews;
    }

    @Override
    public String toString()
    {
        StringBuilder buf = new StringBuilder("RedisClusterHeartBeat ");
        buf.append(" ").append(nodeAddress).append(" ").append(nodeName).append(" ").append(nodeState).append(" ").append(nodeType).append('\n');
        nodeViews.forEach((addr, view) -> buf.append('\t').append(addr).append(' ').append(view).append('\n'));
        return buf.toString();
    }
}
