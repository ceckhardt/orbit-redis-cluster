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

import cloud.orbit.actors.NodeType;
import cloud.orbit.actors.NodeState;
import cloud.orbit.actors.cluster.NodeAddress;

import java.io.Serializable;

/**
 * A <code>RedisClusterNodeView</code> is an immutable snapshot of a single node's "view" of another node as published
 * in a <code>RedisClusterHeartBeat</code>. See also related class <code>RedisClusterNodeTracker</code>, which is the
 * mutable tracker version of this class.
 */
public class RedisClusterNodeView implements Serializable
{
    private final NodeAddress nodeAddress;
    private final NodeType nodeType;
    private final NodeState nodeState;

    // Not-strictly-necessary stats values:
    private final long lastReceivedTimestamp; // is this timeout from the source node, or is it recorded on the dest node? if we're comparing to the dest node's clock, then it should be pulled from the dest node's clock too
    private final int lastReceivedSequenceNumber;

    private final int missedSequenceNumbersCount; // a 'missed' seqnum is between two received heartbeats. if you get 5, then get 8, then missedSequenceNumbersCount += 2;
    private final int longestMissedSequenceNumberStreak;

    public RedisClusterNodeView(
            final NodeAddress nodeAddress,
            final NodeType nodeType,
            final NodeState nodeState,
            final long lastReceivedTimestamp,
            final int lastReceivedSequenceNumber,
            final int missedSequenceNumbersCount,
            final int longestMissedSequenceNumberStreak
    )
    {
        this.nodeAddress = nodeAddress;
        this.nodeType = nodeType;
        this.nodeState = nodeState;
        this.lastReceivedTimestamp = lastReceivedTimestamp;
        this.lastReceivedSequenceNumber = lastReceivedSequenceNumber;
        this.missedSequenceNumbersCount = missedSequenceNumbersCount;
        this.longestMissedSequenceNumberStreak = longestMissedSequenceNumberStreak;
    }

    public NodeAddress getNodeAddress()
    {
        return nodeAddress;
    }

    public NodeType getNodeType()
    {
        return nodeType;
    }

    public NodeState getNodeState()
    {
        return nodeState;
    }

    public long getLastReceivedTimestamp()
    {
        return lastReceivedTimestamp;
    }

    public int getLastReceivedSequenceNumber()
    {
        return lastReceivedSequenceNumber;
    }

    public int getMissedSequenceNumbersCount()
    {
        return missedSequenceNumbersCount;
    }

    public int getLongestMissedSequenceNumberStreak()
    {
        return longestMissedSequenceNumberStreak;
    }

    @Override
    public String toString()
    {
        long now = System.currentTimeMillis();
        return nodeState + " " + nodeType + " (" + (now - lastReceivedTimestamp) + "ms ago)";
    }
}
