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

package cloud.orbit.actors.cluster.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ssedano.hash.JumpConsistentHash;

import cloud.orbit.actors.cluster.RedisClusterConfig;
import cloud.orbit.actors.cluster.impl.lettuce.FstStringObjectCodec;
import cloud.orbit.actors.cluster.impl.lettuce.LettuceClient;
import cloud.orbit.actors.cluster.impl.lettuce.LettucePubSubClient;
import cloud.orbit.concurrent.Task;
import cloud.orbit.exception.UncheckedException;
import io.lettuce.core.pubsub.RedisPubSubListener;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;


/**
 * Created by joeh@ea.com on 2016-12-13.
 */
public class RedisConnectionManager
{

    private final List<LettuceClient<String, Object>> nodeDirectoryClients = new ArrayList<>();
    private final List<LettuceClient<String, Object>> actorDirectoryClients = new ArrayList<>();
    private final List<LettucePubSubClient> messagingClients = new ArrayList<>();
    private static final Logger logger = LoggerFactory.getLogger(RedisConnectionManager.class);

    private final RedisClusterConfig redisClusterConfig;

    public RedisConnectionManager(final RedisClusterConfig redisClusterConfig)
    {
        this.redisClusterConfig = redisClusterConfig;
        final List<String> nodeDirectoryMasters = redisClusterConfig.getNodeDirectoryUris();
        for (final String uri : nodeDirectoryMasters)
        {
            logger.info("Connecting to Redis Node Directory node at '{}'...", uri);
            nodeDirectoryClients.add(createLettuceNodeClient(uri, redisClusterConfig));
        }

        final List<String> actorDirectoryMasters = redisClusterConfig.getActorDirectoryUris();
        for (final String uri : actorDirectoryMasters)
        {
            logger.info("Connecting to Redis Actor Directory node at '{}'...", uri);
            actorDirectoryClients.add(createLettuceActorClient(uri, redisClusterConfig));
        }


        final List<String> messagingMasters = redisClusterConfig.getMessagingUris();
        refreshMessagingTopology(messagingMasters);
    }

    public void refreshMessagingTopology(final List<String> messagingUris) {
        // First add missing clients
        addMissingMessagingClients(messagingUris);

        // Second remove clients that are no longer in the topology
        removeMissingMessagingClients(messagingUris);
    }

    private void addMissingMessagingClients(List<String> messagingUris) {
        List<String> missing = messagingUris.stream().map(url -> {
            if (messagingClients.stream().filter(c -> c.getRedisUrl().equals(url)).findAny().isPresent()) {
                return null;
            }
            return url;
        }).filter(Objects::nonNull).collect(toList());
        missing.forEach(uri -> {
            logger.info("Connecting to Redis messaging node at '{}'...", uri);
            messagingClients.add(createLettucePubSubClient(uri, redisClusterConfig.getRedisPipelineFlushIntervalMillis(), redisClusterConfig.getRedisPipelineFlushCommandCount()));
        });
    }

    private void removeMissingMessagingClients(List<String> messagingUris) {
        Iterator<LettucePubSubClient> itr = messagingClients.iterator();
        while (itr.hasNext()) {
            LettucePubSubClient client = itr.next();
            if (!messagingUris.contains(client.getRedisUrl())) {
                itr.remove();
                client.shutdown();
            }
        }
    }

    public List<LettuceClient<String, Object>> getNodeDirectoryClients()
    {
        return Collections.unmodifiableList(nodeDirectoryClients);
    }

    public List<LettuceClient<String, Object>> getActorDirectoryClients()
    {
        return Collections.unmodifiableList(actorDirectoryClients);
    }

    public List<LettucePubSubClient> getActiveMessagingClients() {
        return messagingClients.stream().filter(LettucePubSubClient::isConnected).collect(toList());
    }

    public LettuceClient<String, Object> getShardedNodeDirectoryClient(final String shardId)
    {
        final int jumpConsistentHash = JumpConsistentHash.jumpConsistentHash(shardId, nodeDirectoryClients.size());
        return nodeDirectoryClients.get(jumpConsistentHash);
    }

    public LettuceClient<String, Object> getShardedActorDirectoryClient(final String shardId)
    {
        final int jumpConsistentHash = JumpConsistentHash.jumpConsistentHash(shardId, actorDirectoryClients.size());
        return actorDirectoryClients.get(jumpConsistentHash);
    }

    public void subscribeToChannel(final String channelId, final RedisPubSubListener<String, Object> statusListener)
    {
        // Note: The same instances of LettucePubSubClient are being used for both node <-> node messaging and for
        // node <-> cluster messaging, which is why messages of both types are delivered to both listeners configured
        // in RedisClusterPeer, which is why they have `instanceof` checks to disambiguate messages.
        // This also means that cluster heartbeats are sensitive to pipelining/batching.
        final List<LettucePubSubClient> localMessagingClients = getActiveMessagingClients();
        logger.info("Subscribing {} Lettuce clients to channel {}", localMessagingClients.size(), channelId);

        final Stream<CompletableFuture<Void>> subscribeTasks = localMessagingClients.stream()
                .map(messagingClient -> subscribeToChannel(messagingClient, channelId, statusListener));

        // Wait for all subscriptions to be completed before returning, flushing to avoid waiting for the next batch timeout.
        localMessagingClients.forEach(LettucePubSubClient::flush);
        Task.allOf(subscribeTasks).join();
    }

    private CompletableFuture<Void> subscribeToChannel(
            final LettucePubSubClient messagingClient,
            final String channelId,
            final RedisPubSubListener<String, Object> statusListener
    )
    {
        return messagingClient.subscribe(channelId, statusListener)
                .exceptionally(e ->
                {
                    logger.error("Error subscribing to channel", e);
                    return null;
                });
    }


    public void sendMessageToChannel(final String channelId, final Object msg)
    {
        final List<LettucePubSubClient> localMessagingClients = getActiveMessagingClients();
        sendMessageToChannel(channelId, msg, localMessagingClients, 1);
    }

    private void sendMessageToChannel(final String channelId, final Object msg, final List<LettucePubSubClient> localMessagingClients, final int attempt)
    {
        final int activeClientCount = localMessagingClients.size();
        if (activeClientCount == 0)
        {
            logger.error("Failed to send message to channel '{}', no redis messaging instances were available after {} attempts.", channelId, attempt);
            return;
        }

        final int randomIndex = ThreadLocalRandom.current().nextInt(activeClientCount);
        final LettucePubSubClient client = localMessagingClients.remove(randomIndex);

        client.publish(channelId, msg)
                .whenComplete((numClientsReceived, exception) -> {
                    if (exception != null)
                    {
                        logger.error("Failed to send message to channel '{}'", channelId, exception);
                    }
                    else if (numClientsReceived == 0)
                    {
                        if (attempt >= redisClusterConfig.getMessageSendAttempts())
                        {
                            logger.error("Failed to send message to channel '{}' after {} attempts.", channelId, attempt);
                        }
                        else
                        {
                            logger.warn("Failed to send message to channel '{}' on attempt {}. Retrying...", channelId, attempt);
                            sendMessageToChannel(channelId, msg, localMessagingClients, attempt + 1);
                        }
                    }
                });
    }

    public void shutdownConnections()
    {
        nodeDirectoryClients.forEach(LettuceClient::shutdown);
        actorDirectoryClients.forEach(LettuceClient::shutdown);
        messagingClients.forEach(LettucePubSubClient::shutdown);
    }

    private LettucePubSubClient createLettucePubSubClient(final String uri, final long pipelineFlushIntervalMillis, final int pipelineFlushCount)
    {
        return new LettucePubSubClient(this.resolveUri(uri), pipelineFlushIntervalMillis, pipelineFlushCount);
    }

    private LettuceClient<String, Object> createLettuceActorClient(final String uri, final RedisClusterConfig config)
    {
        return new LettuceClient<>(this.resolveUri(uri), new FstStringObjectCodec(), config.getConnectionTimeout(), config.getUseClusterForDirectoryNodes(), config.getUseElasticacheForDirectoryNodes());
    }

    private LettuceClient<String, Object> createLettuceNodeClient(final String uri, final RedisClusterConfig config)
    {
        return new LettuceClient<>(this.resolveUri(uri), new FstStringObjectCodec(), config.getConnectionTimeout(), config.getUseClusterForDirectoryNodes(), config.getUseElasticacheForDirectoryNodes());
    }

    private  String resolveUri(final String uri)
    {
        // Resolve URI
        final URI realUri = URI.create(uri);
        if (!realUri.getScheme().equalsIgnoreCase("redis"))
        {
            throw new UncheckedException("Invalid Redis URI.");
        }
        String host = realUri.getHost();
        if (host == null) host = "localhost";
        Integer port = realUri.getPort();
        if (port == -1) port = 6379;
        return "redis://" + host + ":" + port;
    }

}
