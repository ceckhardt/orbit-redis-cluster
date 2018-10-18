/*
 Copyright (C) 2016 Electronic Arts Inc.  All rights reserved.

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

package cloud.orbit.actors.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cloud.orbit.actors.Actor;
import cloud.orbit.actors.NodeState;
import cloud.orbit.actors.NodeType;
import cloud.orbit.actors.cluster.heartbeat.RedisClusterHeartBeat;
import cloud.orbit.actors.cluster.impl.RedisConnectionManager;
import cloud.orbit.actors.cluster.impl.RedisKeyGenerator;
import cloud.orbit.actors.cluster.impl.RedisMsg;
import cloud.orbit.actors.cluster.impl.RedisShardedMap;
import cloud.orbit.actors.cluster.state.RedisClusterTracker;
import cloud.orbit.concurrent.Task;
import cloud.orbit.tuples.Pair;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.RedisPubSubListener;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * The <code>RedisClusterPeer</code> implements a <code>ClusterPeer</code> backed by Redis.
 *
 * Nodes communicate about the state of the cluster by periodically sending heartbeats to a shared pubsub topic.
 * Heartbeats contain each node's own view of the cluster, as well as its own state. Updates that change the topology
 * of the cluster (such as adding or losing nodes) are propagated up to Orbit's <code>Hosting</code> layer by the
 * <code>ViewListener</code>.
 *
 * Messages between nodes are also sent over sharded pub-sub topics.
 *
 */
public class RedisClusterPeer implements ClusterPeer
{
    private static Logger logger = LoggerFactory.getLogger(RedisClusterPeer.class);
    private ViewListener viewListener;
    private MessageListener messageListener;
    private NodeAddress localAddress = new NodeAddressImpl(UUID.randomUUID());
    private String clusterName;
    private RedisClusterConfig config;
    private RedisConnectionManager redisConnectionManager;

    private final ConcurrentMap<String, ConcurrentMap<?, ?>> cacheManager = new ConcurrentHashMap<>();

    private final RedisClusterTracker clusterTracker;

    private volatile ClusterView latestClusterView;


    public RedisClusterPeer(final RedisClusterConfig config)
    {
        this.config = config;

        final Collection<Class<? extends Actor>> actorInterfaces = config.getActorClassFinder().findActorInterfaces(p -> true);

        final Set<String> hostableInterfaces = actorInterfaces.stream()
                .map(Class::getName)
                .collect(Collectors.toSet());

        this.clusterTracker = new RedisClusterTracker(config, this.localAddress, hostableInterfaces);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> ConcurrentMap<K, V> getCache(final String name)
    {
        final String realName = RedisKeyGenerator.key("shardedMap", Pair.of("cluster", clusterName), Pair.of("mapName", name));
        ConcurrentMap<?, ?> result = cacheManager.get(realName);
        if (result == null)
        {
            ConcurrentMap<?, ?>  targetMap = new RedisShardedMap<K, V>(realName, redisConnectionManager.getActorDirectoryClients(), config.getShardingBuckets());
            result = cacheManager.putIfAbsent(realName, targetMap);
            if (result == null)
            {
                result = targetMap;
            }
        }
        return (ConcurrentMap<K, V>) result;
    }

    @Override
    public NodeAddress localAddress()
    {
        return localAddress;
    }

    @Override
    public Task<?> join(final String clusterName, final String nodeName, final NodeType nodeType)
    {
        logger.info("Joining Redis Cluster '{}' as node '{}' [{}]...", clusterName, nodeName, localAddress.asUUID().toString());
        this.clusterTracker.setNodeName(nodeName);

        this.clusterName = clusterName;
        this.redisConnectionManager = new RedisConnectionManager(config);

        this.clusterTracker.setNodeState(NodeState.RUNNING);
        this.clusterTracker.setNodeType(NodeType.CLIENT);

        /*
        Note: Because the `RedisConnectionManager` maintains a single list of `LettucePubSubClient`s to be used for all
        messaging, both node->node messages and node->cluster heartbeats are multiplexed over the same underlying TCP/
        Redis connection. Messages must therefore be discriminated by the `RedisPubSubListener` instance based on either
        the channel or the message object type -- here, we have chosen to discriminate based on message type.
         */

        // Subscribe to Cluster-HeartBeat channel
        final String clusterChannelKey = getClusterChannelKey(clusterName);
        logger.info("Joining topic '{}'", clusterChannelKey);
        redisConnectionManager.subscribeToChannel(clusterChannelKey, new RedisPubSubAdapter<String, Object>()
        {
            @Override
            public void message(final String channel, final Object redisMsg)
            {
                if ( redisMsg instanceof RedisClusterHeartBeat )
                {
                    receiveHeartBeat((RedisClusterHeartBeat)redisMsg);
                }
            }
        });

        // Wait until the cluster agrees that this node is in the cluster.
        while ( ! clusterTracker.isLocalNodeInCluster() && ! this.clusterTracker.isThisNodeDead() ) {
            pulse();
            sleep();
        }

        logger.info("Done joining the cluster as CLIENT");

        // If required, upgrade ourselves to server mode and wait until the cluster agrees.
        if ( nodeType == NodeType.SERVER ) {
            logger.info("Upgrading from CLIENT to SERVER");

            clusterTracker.setNodeType(NodeType.SERVER);
            pulse();

            logger.info("Done upgrading from CLIENT to SERVER");
        }

        // Subscribe to Orbit Messaging Pub Sub
         logger.info("Subscribing to messages...");
        final String nodeKey = RedisKeyGenerator.nodeKey(clusterName, localAddress.toString());
        RedisPubSubListener<String, Object> listener = new RedisPubSubAdapter<String, Object>()
        {
            @Override
            public void message(String channel, Object redisMsg)
            {
                if ( redisMsg instanceof RedisMsg )
                {
                    receiveMessage((RedisMsg) redisMsg);
                }
            }
        };
        redisConnectionManager.subscribeToChannel(nodeKey, listener);

        logger.info("Done joining cluster!");
        return Task.done();
    }

    private void sleep()
    {
        try
        {
            Thread.sleep(1000L);
        } catch (InterruptedException ignored) {}
    }

    @Override
    public Task<?> notifyStateChange(final NodeState newNodeState)
    {
        changeLocalNodeState(newNodeState);
        return Task.done();
    }

    public void refreshMessageTopology(final List<String> messagingUris) {
        redisConnectionManager.refreshMessagingTopology(messagingUris);
    }

    private String getClusterChannelKey(final String clusterName) {
        return RedisKeyGenerator.clusterKey(clusterName);
    }

    private void publishHeartBeat() {
        final String clusterChannelKey = getClusterChannelKey(clusterName);
        final RedisClusterHeartBeat heartBeat = clusterTracker.createHeartBeat();
        redisConnectionManager.sendMessageToChannel(clusterChannelKey, heartBeat);
    }

    private void receiveHeartBeat(RedisClusterHeartBeat heartBeat) {
        // Shift the work onto an Orbit (rather than Lettuce) thread.
        Task.runAsync(() -> receiveHeartBeatInternal(heartBeat), config.getCoreExecutorService())
                .exceptionally((e) ->
                {
                    logger.error("Error receiving heartbeat", e);
                    return null;
                });
    }

    private void receiveHeartBeatInternal ( final RedisClusterHeartBeat heartBeat )
    {
        logger.trace("receiveHeartBeat {}", heartBeat);

        // Apply the new HeartBeat message to our internal state.
        boolean clusterViewChanged = clusterTracker.receiveHeartBeat(heartBeat);

        if ( clusterTracker.isLocalNodeInCluster() ) {
            if ( clusterViewChanged )
            {
                pushNewClusterView();
            }
        }
    }

    @Override
    public void sendMessage(final NodeAddress toAddress, final byte[] message)
    {
        final RedisMsg redisMsg = new RedisMsg(localAddress.asUUID(), message);
        final String targetNodeKey = RedisKeyGenerator.nodeKey(clusterName, toAddress.toString());
        redisConnectionManager.sendMessageToChannel(targetNodeKey, redisMsg);
    }

    public void receiveMessage(final RedisMsg rawMessage)
    {
        Task.runAsync(() ->
                {
                    final NodeAddress nodeAddr = new NodeAddressImpl(rawMessage.getSenderAddress());
                    messageListener.receive(nodeAddr, rawMessage.getMessageContents());
                },
                config.getCoreExecutorService()
        )
                .exceptionally((e) ->
                {
                    logger.error("Error receiving message", e);
                    return null;
                });
    }

    @Override
    public Task pulse()
    {
        final Set<NodeAddress> deadNodes = clusterTracker.scanForDeadNodes();

        // If this node isn't hearing its own heartbeats, then it must be a zombie and must die immediately.
        final boolean thisNodeIsDead = clusterTracker.isThisNodeDead();
        if ( thisNodeIsDead ) {
            logger.error("FATAL: Node {} detected itself as dead. Exiting to restart.", localAddress);
            System.exit(-1);
        }

        // If we're alive, we can eventually forget about some long-dead nodes.
        final boolean culledAnyNodes = clusterTracker.cullLongDeadNodes();

        // Note: This ordering is important. We should only update the cluster view if this node isn't dead.
        if ( ! deadNodes.isEmpty() ) {
            logger.info("Detected dead nodes {}; updating view", deadNodes);
            pushNewClusterView();
        } else if ( culledAnyNodes ) {
            logger.info("Culled some dead nodes; updating view");
            pushNewClusterView();
        }

        publishHeartBeat();

        return Task.done();
    }

    private void pushNewClusterView()
    {
        final ClusterView clusterView = clusterTracker.createClusterView();
        logger.info("ClusterView = {}", clusterView);
        this.latestClusterView = clusterView;
        viewListener.onViewChange(clusterView);
    }

    @Override
    public void leave()
    {
        // Tell other nodes that this node has stopped
        changeLocalNodeState(NodeState.STOPPED);
        redisConnectionManager.shutdownConnections();
    }

    private void changeLocalNodeState ( final NodeState newNodeState ) {
        // Note: since changeLocalNodeState is only used for RUNNING -> STOPPING and STOPPING -> STOPPED changes, we
        // don't wait for other nodes to agree here; we just wait to make sure that we've either (a) sent an updated
        // heartbeat correctly, or (b) we have died and can just give up.
        this.clusterTracker.setNodeState(newNodeState);
        do
        {
            pulse();
            sleep();
        } while ( ! this.clusterTracker.isThisNodeInState(newNodeState) && ! this.clusterTracker.isThisNodeDead() );
    }

    @Override
    public void registerMessageReceiver(final MessageListener messageListener)
    {
        this.messageListener = messageListener;
    }

    @Override
    public void registerViewListener(final ViewListener viewListener)
    {
        this.viewListener = viewListener;
    }

    public ClusterView getLatestClusterView()
    {
        return this.latestClusterView;
    }

    public RedisClusterTracker getClusterTracker()
    {
        return this.clusterTracker;
    }
}
