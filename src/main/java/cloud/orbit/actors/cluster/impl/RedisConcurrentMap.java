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

import cloud.orbit.actors.cluster.impl.lettuce.LettuceClient;
import cloud.orbit.exception.NotImplementedException;
import io.lettuce.core.ScriptOutputType;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;


public class RedisConcurrentMap<K, V> implements ConcurrentMap<K, V>
{
    private final String name;
    private final LettuceClient<Object, Object> redisClient;

    public RedisConcurrentMap(final String name, final LettuceClient<Object, Object> redisClient) {
        this.name = name;
        this.redisClient = redisClient;
    }

    @Override
    public int size()
    {
        return redisClient.getAsyncCommands().hlen(name).toCompletableFuture().join().intValue();
    }

    @Override
    public boolean isEmpty()
    {
        return size() == 0;
    }

    @Override
    public boolean containsKey(final Object key)
    {
        return redisClient.getAsyncCommands().hexists(name, key).toCompletableFuture().join();
    }

    @Override
    public boolean containsValue(final Object value)
    {
        throw new NotImplementedException();
    }

    @Override
    public V get(final Object key)
    {
        return (V)redisClient.getAsyncCommands().hget(name, key).toCompletableFuture().join();
    }

    @Override
    public V put(final K key, final V value)
    {
        final String script = "local v = redis.call('hget', KEYS[1], KEYS[2]);\n"
                + "redis.call('hset', KEYS[1], KEYS[2], KEYS[3]);\n"
                + "return v\n";
        return (V)this.redisClient.getAsyncCommands().eval(script, ScriptOutputType.VALUE, name, key, value)
                .toCompletableFuture().join();
    }

    @Override
    public V remove(final Object key)
    {
        final String script = "local v = redis.call('hget', KEYS[1], KEYS[2]); "
                + "redis.call('hdel', KEYS[1], KEYS[2]); "
                + "return v";
        return (V)this.redisClient.getAsyncCommands().eval(script, ScriptOutputType.VALUE, name, key)
                .toCompletableFuture().join();
    }

    @Override
    public boolean remove(final Object key, final Object oldValue)
    {
        final String script = "if redis.call('hget', KEYS[1], KEYS[2]) == KEYS[3] then\n"
                + "  return redis.call('hdel', KEYS[1], KEYS[2])\n"
                + "else\n"
                + "  return 0\n"
                + "end\n";
        return (Boolean)this.redisClient.getAsyncCommands().eval(script, ScriptOutputType.BOOLEAN, name, key, oldValue)
                .toCompletableFuture().join();

    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> m) {
        throw new NotImplementedException();
    }

    @Override
    public void clear()
    {
        redisClient.getAsyncCommands().del(name).toCompletableFuture().join();
    }

    @Override
    public Set<K> keySet()
    {
        throw new NotImplementedException();
    }

    @Override
    public Collection<V> values()
    {
        throw new NotImplementedException();
    }

    @Override
    public Set<Entry<K, V>> entrySet()
    {
        throw new NotImplementedException();
    }


    @Override
    public V putIfAbsent(final K key, final V value)
    {
        final String script = "if redis.call('hsetnx', KEYS[1], KEYS[2], KEYS[3]) == 1 then\n"
                + "  return nil\n"
                + "else \n"
                + "  return redis.call('hget', KEYS[1], KEYS[2])\n"
                + "end";
        return (V)this.redisClient.getAsyncCommands().eval(script, ScriptOutputType.VALUE, name, key, value)
                .toCompletableFuture().join();

    }

    @Override
    public boolean replace(final Object key, final Object oldValue, final Object newValue)
    {
        final String script = "if redis.call('hget', KEYS[1], KEYS[2]) == KEYS[3] then\n"
                + "  redis.call('hset', KEYS[1], KEYS[2], KEYS[4]);\n"
                + "  return 1;\n"
                + "else\n"
                + "  return 0;\n"
                + "end\n";

        return (Boolean)this.redisClient.getAsyncCommands().eval(script, ScriptOutputType.BOOLEAN, name, key, oldValue, newValue)
                .toCompletableFuture().join();
    }

    @Override
    public V replace(final K key, final V value)
    {
        final String script = "if redis.call('hexists', KEYS[1], KEYS[2]) == 1 then\n"
                + "  local v = redis.call('hget', KEYS[1], KEYS[2]); \n"
                + "  redis.call('hset', KEYS[1], KEYS[2], KEYS[3]);\n"
                + "  return v;\n"
                + "else\n"
                + "  return nil;\n"
                + "end\n";
        return (V)this.redisClient.getAsyncCommands().eval(script, ScriptOutputType.VALUE, name, key, value)
                .toCompletableFuture().join();

    }
}
