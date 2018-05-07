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

import cloud.orbit.actors.cluster.impl.lettuce.LettuceOrbitClient;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;


public class RedisConcurrentMap implements ConcurrentMap<Object, Object>
{
    private final String name;
    private final LettuceOrbitClient redisClient;

    public RedisConcurrentMap(final String name, final LettuceOrbitClient redisClient) {
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
        return redisClient.getAsyncCommands().hexists(name, Objects.toString(key)).toCompletableFuture().join();
    }

    @Override
    public boolean containsValue(final Object value)
    {
        throw new IllegalStateException("NOT IMPLEMENTED");
    }

    @Override
    public Object get(final Object key)
    {
        return redisClient.getAsyncCommands().hget(name, Objects.toString(key)).toCompletableFuture().join();
    }

    @Override
    public Object put(final Object key, final Object value)
    {
        return redisClient.getAsyncCommands().hset(name, Objects.toString(key), value).toCompletableFuture().join();
    }

    @Override
    public Object remove(final Object key)
    {
        return redisClient.getAsyncCommands().hdel(name, Objects.toString(key)).toCompletableFuture().join();
    }

    @Override
    public boolean remove(final Object key, final Object oldValue)
    {
        // TODO this is not atomic... need to make a lua command to do this
        Object r = this.redisClient.getAsyncCommands().hget(name, Objects.toString(key)).toCompletableFuture().join();
        if (r != null && Objects.equals(r, oldValue)) {
            this.redisClient.getAsyncCommands().hdel(name, Objects.toString(key)).toCompletableFuture().join();
            return true;
        } else {
            return false;
        }

    }

    @Override
    public void putAll(final Map<? extends Object, ?> m) {
        throw new IllegalStateException();
    }

    @Override
    public void clear()
    {
        redisClient.getAsyncCommands().del(name).toCompletableFuture().join();
    }

    @Override
    public Set<Object> keySet()
    {
        throw new IllegalStateException("NOT IMPLEMENTED");
    }

    @Override
    public Collection<Object> values()
    {
        throw new IllegalStateException("NOT IMPLEMENTED");
    }

    @Override
    public Set<Entry<Object, Object>> entrySet()
    {
        throw new IllegalStateException("NOT IMPLEMENTED");
    }


    @Override
    public Object putIfAbsent(final Object key, final Object value)
    {
        return this.redisClient.getAsyncCommands().hsetnx(name, Objects.toString(key), value).toCompletableFuture().join();
    }

    @Override
    public boolean replace(final Object key, final Object oldValue, final Object newValue)
    {
        // TODO this is not atomic... need to make a lua command to do this
        Object r = this.redisClient.getAsyncCommands().hget(name, Objects.toString(key)).toCompletableFuture().join();
        if (r != null && Objects.equals(r, oldValue)) {
            this.redisClient.getAsyncCommands().hset(name, Objects.toString(key), newValue).toCompletableFuture().join();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Object replace(final Object key, final Object value)
    {
        // TODO this is not atomic... need to make a lua command to do this
        Object r = this.redisClient.getAsyncCommands().hget(name, Objects.toString(key)).toCompletableFuture().join();
        if (r != null) {
            this.redisClient.getAsyncCommands().hset(name, Objects.toString(key), value).toCompletableFuture().join();
            return r;
        } else {
            return null;
        }
    }
}
