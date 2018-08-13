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

package cloud.orbit.actors.cluster.impl.lettuce;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.resource.DirContextDnsResolver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LettuceClient<K, V>
{
    private static Logger logger = LoggerFactory.getLogger(LettuceClient.class);

    private final RedisClusterClient redisClusterClient;
    private final RedisClient redisSingleClient;

    private final RedisCodec<K, V> codec;
    private final String redisUri;

    private BaseRedisAsyncCommands<K, V> asyncCommands;

    public LettuceClient(final String resolvedUri, final RedisCodec<K, V> codec, final long timeoutMillis, final boolean clusterSupport, final boolean useElasticache)
    {
        this.redisUri = resolvedUri;

        this.codec = codec;


        if (clusterSupport) {
            this.redisClusterClient = RedisClusterClient.create(DefaultClientResources.builder()
                    .dnsResolver(new DirContextDnsResolver())
                    .build(), redisUri);
            this.redisClusterClient.setDefaultTimeout(Duration.ofMillis(timeoutMillis));
            final ClusterClientOptions.Builder optionsBuilder = ClusterClientOptions.builder()
                    .topologyRefreshOptions(ClusterTopologyRefreshOptions.builder()
                            .enablePeriodicRefresh(false)
                            .enableAllAdaptiveRefreshTriggers()
                            .build());

            if (useElasticache) {
                optionsBuilder.validateClusterNodeMembership(false);
            }
            this.redisClusterClient.setOptions(optionsBuilder.build());
            this.asyncCommands = redisClusterClient.connect(this.codec).async();
            this.redisSingleClient = null;
        } else {
            this.redisSingleClient = RedisClient.create(redisUri);
            this.redisSingleClient.setDefaultTimeout(Duration.ofMillis(timeoutMillis));
            this.asyncCommands = redisSingleClient.connect(this.codec).async();
            this.redisClusterClient = null;
        }
    }

    public String getRedisUri()
    {
        return this.redisUri;
    }

    public RedisClusterAsyncCommands<K, V> commands() {
        return (RedisClusterAsyncCommands<K, V>) asyncCommands;
    }

    public CompletableFuture<V> get(final K key) {
        return commands().get(key).toCompletableFuture();
    }

    public CompletableFuture<String> set(final K key, final V value) {
        return commands().set(key, value).toCompletableFuture();
    }

    public CompletableFuture<String> set(final K key, final V value, final long expireMs) {
        if (expireMs < 1) {
            return this.set(key, value);
        }
        return commands().set(key, value, SetArgs.Builder.px(expireMs)).toCompletableFuture();
    }

    public CompletableFuture<Long> del(final K key) {
        return commands().del(key).toCompletableFuture();
    }

    public CompletableFuture<List<String>> scan(final String matches) {
        // Batches of 1000?  TODO measure and adjust batch size if necessary
        return scan(matches, 1000);
    }

    public CompletableFuture<List<String>> scan(
            final String matches,
            final long count) {

        final List<String> existing = new ArrayList<>();

        return this.scan(existing, matches, count);

    }

    private CompletableFuture<List<String>> scan(
            final List<String> existing,
            final String matches,
            final long count) {

        return commands().scan(ScanArgs.Builder.limit(count).match(matches))
                .toCompletableFuture()
                .thenCompose(initialCursor -> this.scan(initialCursor, existing, matches, count));
    }

    private CompletableFuture<List<String>> scan(
            final KeyScanCursor<K> cursor,
            final List<String> existing,
            final String matches,
            final long count) {

        existing.addAll(cursor.getKeys().stream()
                .map(object -> Objects.toString(object, null))
                .collect(Collectors.toList()));

        if (cursor.isFinished()) {
            return CompletableFuture.completedFuture(existing);
        }

        return commands().scan(cursor, ScanArgs.Builder.limit(count).match(matches))
                .toCompletableFuture()
                .thenCompose(newCursor -> this.scan(newCursor, existing, matches, count));

    }

    public void shutdown() {
        try {
            if (redisClusterClient != null)
            {
                this.redisClusterClient.shutdown();
            }
        } catch (Exception e) {
            logger.error("Shutdown redisClusterClient", e);
        }
        try {
            if (redisSingleClient != null)
            {
                this.redisSingleClient.shutdown();
            }
        } catch (Exception e) {
            logger.error("Shutdown redisSingleClient", e);
        }
    }
}
