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

package cloud.orbit.actors.cluster.impl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import cloud.orbit.actors.cluster.IntegrationTest;
import cloud.orbit.actors.cluster.RedisClusterConfig;
import cloud.orbit.actors.cluster.impl.lettuce.FstObjectCodec;
import cloud.orbit.actors.cluster.impl.lettuce.LettuceClient;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Ignore
public class ConnectionTest
{
    RedisClusterConfig config;
    RedisConnectionManager connectionManager;

    FstObjectCodec codec = new FstObjectCodec();

    boolean setup = false;
    String clusterName = "testcluster";

    @Before
    public void setup()
    {
        if (setup)
        {
            return;
        }

        config = new RedisClusterConfig();
        config.setActorDirectoryUris(Arrays.asList("redis://localhost:6379"));
        config.setNodeDirectoryUris(Arrays.asList("redis://localhost:6379"));
        config.setMessagingUris(Arrays.asList("redis://localhost:6379"));
        config.setUseClusterForDirectoryNodes(false);
        config.setUseElasticacheForDirectoryNodes(false);
        connectionManager = new RedisConnectionManager(config);
        setup = true;
    }

    @Test
    @Category(IntegrationTest.class)
    public void testRedisTopology()
    {
        final String channelId = UUID.randomUUID().toString();
        Assert.assertEquals(1, connectionManager.getActiveMessagingClients().size());
        connectionManager.sendMessageToChannel(channelId, "test message");
        connectionManager.refreshMessagingTopology(Arrays.asList("redis://localhost:6379", "redis://localhost:6380"));
        Assert.assertEquals(2, connectionManager.getActiveMessagingClients().size());
        connectionManager.sendMessageToChannel(channelId, "test message");
        connectionManager.refreshMessagingTopology(Arrays.asList("redis://localhost:6379"));
        Assert.assertEquals(1, connectionManager.getActiveMessagingClients().size());
        connectionManager.sendMessageToChannel(channelId, "test message");
    }


    @Category(IntegrationTest.class)
    public void testManualFail()
    {
        final String channelId = UUID.randomUUID().toString();
        connectionManager.refreshMessagingTopology(Arrays.asList("redis://localhost:6379", "redis://localhost:6380"));
        Assert.assertEquals(2, connectionManager.getActiveMessagingClients().size());

        // Kill one of the redis message nodes in bash while this is running
        for (int i = 0; i < 10; i++) {
            sleep(5, TimeUnit.SECONDS);
            System.out.println("Active messaging clients: " + connectionManager.getActiveMessagingClients().size());
            connectionManager.sendMessageToChannel(channelId, "test message");
            if (connectionManager.getActiveMessagingClients().size() == 1) {
                break;
            }
        }
        Assert.assertEquals(1, connectionManager.getActiveMessagingClients().size());
    }

    private void sleep(final long duration, final TimeUnit unit)
    {
        try
        {
            Thread.sleep(unit.toMillis(duration));
        } catch (Exception e) {}
    }

    @Test
    @Category(IntegrationTest.class)
    public void testMap()
    {
        List<LettuceClient<String, Object>> clients = connectionManager.getActorDirectoryClients();
        Assert.assertFalse(clients.isEmpty());
        RedisShardedMap map = new RedisShardedMap("test.map", connectionManager.getActorDirectoryClients(), 10);
        map.clear();
        Assert.assertTrue(map.isEmpty());
        String k = "key";
        String v = "value";
        map.put(k, v);
        String value = (String) map.get(k);
        Assert.assertEquals(v, value);
        Assert.assertFalse(map.isEmpty());
        Assert.assertTrue(map.containsKey(k));
        map.remove("key");
        Assert.assertTrue(map.isEmpty());
    }
}
