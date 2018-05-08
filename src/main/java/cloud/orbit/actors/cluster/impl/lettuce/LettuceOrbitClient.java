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

import cloud.orbit.actors.cluster.impl.RedisConcurrentMap;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LettuceOrbitClient
{
    private static Logger logger = LoggerFactory.getLogger(LettuceOrbitClient.class);

    private final RedisClient redisClient;
    private final StatefulRedisPubSubConnection<String, Object> redisSubscribingConnection;
    private final RedisPubSubAsyncCommands<String, Object> redisSubscribingAsyncCommands;
    private final StatefulRedisPubSubConnection<String, Object> redisPublishingConnection;
    private final RedisPubSubAsyncCommands<String, Object> redisPublishingAsyncCommands;

    private final AtomicInteger commandCounter = new AtomicInteger(1);
    private final AtomicBoolean flushed = new AtomicBoolean(false);
    private final int pipelineFlushCount;

    private RedisAsyncCommands<String, Object> asyncCommands;

    private ScheduledExecutorService executor;

    public LettuceOrbitClient(final String resolvedUri, long pipelineFlushIntervalMillis, int pipelineFlushCount)
    {
        this.pipelineFlushCount = pipelineFlushCount;
        boolean autoFlush = pipelineFlushIntervalMillis < 1;

        this.redisClient = RedisClient.create(resolvedUri);

        this.redisSubscribingConnection = this.redisClient.connectPubSub(new FstSerializedObjectCodec());
        this.redisSubscribingAsyncCommands = this.redisSubscribingConnection.async();
        this.redisSubscribingAsyncCommands.setAutoFlushCommands(true); // No redis pipelining on subscriptions

        this.redisPublishingConnection = this.redisClient.connectPubSub(new FstSerializedObjectCodec());
        this.redisPublishingAsyncCommands = this.redisPublishingConnection.async();
        this.redisPublishingAsyncCommands.setAutoFlushCommands(autoFlush);

        this.asyncCommands = this.redisClient.connect(new FstSerializedObjectCodec()).async();
        setupExecutor(pipelineFlushIntervalMillis);
    }

    /*
        Single thread executor, to clean up(flush) redis pipeline in case of low command activity
     */
    private void setupExecutor(long pipelineFlushIntervalMillis) {
        if (pipelineFlushIntervalMillis < 1) {
            return;
        }
        this.executor = Executors.newSingleThreadScheduledExecutor();
        Runnable task = () -> {
            try {
                if (!flushed.getAndSet(false))
                {
                    flush();
                }
            } catch (Exception e) {
                logger.error("Error flushing commands", e);
            }
        };

        executor.scheduleAtFixedRate(task, pipelineFlushIntervalMillis, pipelineFlushIntervalMillis / 2, TimeUnit.MILLISECONDS);
    }

    public CompletableFuture<Void> subscribe(final String channelId, final RedisPubSubListener<String, Object> messageListener)
    {
        if (this.redisSubscribingConnection.isOpen())
        {
            this.redisSubscribingConnection.addListener(messageListener);
            return this.redisSubscribingAsyncCommands.subscribe(channelId).toCompletableFuture();
        }
        else
        {
            logger.error("Error subscribing to channel [{}]", channelId);
            final CompletableFuture<Void> result = new CompletableFuture<>();
            result.completeExceptionally(new IllegalStateException("Error subscribing to channel..."));
            return result;
        }
    }

    public CompletableFuture<Long> publish(final String channelId, final Object redisMsg)
    {
        if (this.redisPublishingConnection.isOpen())
        {
            return this.redisPublishingAsyncCommands.publish(channelId, redisMsg).toCompletableFuture()
                    .thenApply(r -> {
                        this.checkFlush();
                        return r;
                    });
        }
        else
        {
            logger.error("Error publishing message to channel [{}]", channelId);
            final CompletableFuture<Long> result = new CompletableFuture<>();
            result.completeExceptionally(new IllegalStateException("Error publishing to channel..."));
            return result;
        }
    }

    private void checkFlush()
    {
        if (needsFlush())
        {
            flush();
        }
    }

    private void flush() {
        redisPublishingAsyncCommands.flushCommands();
        flushed.set(true);
    }

    boolean needsFlush()
    {
        return pipelineFlushCount > 0 && commandCounter.updateAndGet(n -> (n >= pipelineFlushCount) ? 1 : n + 1 ) == 1;
    }

    public boolean isConnected()
    {
        return this.redisSubscribingConnection.isOpen() && this.redisPublishingConnection.isOpen();
    }

    public CompletableFuture<Object> get(final String key) {
        return this.asyncCommands.get(key).toCompletableFuture();
    }

    public CompletableFuture<String> set(final String key, Object value) {
        return this.asyncCommands.set(key, value).toCompletableFuture();
    }

    public CompletableFuture<String> set(final String key, Object value, long expireMs) {
        if (expireMs < 1) {
            return this.set(key, value);
        }
        return this.asyncCommands.set(key, value, SetArgs.Builder.px(expireMs)).toCompletableFuture();
    }

    public CompletableFuture<Long> del( final String key) {
        return this.asyncCommands.del(key).toCompletableFuture();
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

        return this.asyncCommands.scan(ScanArgs.Builder.limit(count).match(matches))
                .toCompletableFuture()
                .thenCompose(initialCursor -> this.scan(initialCursor, existing, matches, count));

    }

    private CompletableFuture<List<String>> scan(
            final KeyScanCursor<String> cursor,
            final List<String> existing,
            final String matches,
            final long count) {

        existing.addAll(cursor.getKeys());

        if (cursor.isFinished()) {
            return CompletableFuture.completedFuture(existing);
        }

        return this.asyncCommands.scan(cursor, ScanArgs.Builder.limit(count).match(matches))
                .toCompletableFuture()
                .thenCompose(newCursor -> this.scan(newCursor, existing, matches, count));

    }

    public RedisConcurrentMap getMap(final String name) {
        return new RedisConcurrentMap(name, this);
    }

    public RedisAsyncCommands<String, Object> getAsyncCommands() {
        if (this.asyncCommands != null && this.asyncCommands.isOpen()) {
            return this.asyncCommands;
        }
        this.asyncCommands = this.redisClient.connect(new FstSerializedObjectCodec()).async();
        return this.asyncCommands;
    }
    public void shutdown()
    {
        this.redisSubscribingConnection.close();
        this.redisPublishingConnection.close();

        this.redisClient.shutdown();
    }
}
