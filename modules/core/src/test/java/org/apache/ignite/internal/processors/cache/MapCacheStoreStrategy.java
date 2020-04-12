/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiInClosure;

/**
 * {@link TestCacheStoreStrategy} implemented as a wrapper around {@link #MAP}
 */
public class MapCacheStoreStrategy implements TestCacheStoreStrategy {
    /** Removes counter. */
    private static final AtomicInteger REMOVES = new AtomicInteger();

    /** Writes counter. */
    private static final AtomicInteger WRITES = new AtomicInteger();

    /** Reads counter. */
    private static final AtomicInteger READS = new AtomicInteger();

    /** Store map. */
    private static final Map<Object, Object> MAP = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public int getReads() {
        return READS.get();
    }

    /** {@inheritDoc} */
    @Override public int getWrites() {
        return WRITES.get();
    }

    /** {@inheritDoc} */
    @Override public int getRemoves() {
        return REMOVES.get();
    }

    /** {@inheritDoc} */
    @Override public int getStoreSize() {
        return MAP.size();
    }

    /** {@inheritDoc} */
    @Override public void resetStore() {
        MAP.clear();

        READS.set(0);
        WRITES.set(0);
        REMOVES.set(0);
    }

    /** {@inheritDoc} */
    @Override public void putToStore(Object key, Object val) {
        MAP.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public void putAllToStore(Map<?, ?> data) {
        MAP.putAll(data);
    }

    /** {@inheritDoc} */
    @Override public Object getFromStore(Object key) {
        return MAP.get(key);
    }

    /** {@inheritDoc} */
    @Override public void removeFromStore(Object key) {
        MAP.remove(key);
    }

    /** {@inheritDoc} */
    @Override public boolean isInStore(Object key) {
        return MAP.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public void updateCacheConfiguration(CacheConfiguration<Object, Object> cfg) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Factory<? extends CacheStore<Object, Object>> getStoreFactory() {
        return FactoryBuilder.factoryOf(MapCacheStore.class);
    }

    /** Serializable {@link #MAP} backed cache store factory */
    public static class MapStoreFactory implements Factory<CacheStore<Object, Object>> {
        /** {@inheritDoc} */
        @Override public CacheStore<Object, Object> create() {
            return new MapCacheStore();
        }
    }

    /** {@link CacheStore} backed by {@link #MAP} */
    public static class MapCacheStore extends CacheStoreAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, Object... args) {
            for (Map.Entry<Object, Object> e : MAP.entrySet())
                clo.apply(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            READS.incrementAndGet();
            return MAP.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> e) {
            WRITES.incrementAndGet();
            MAP.put(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            REMOVES.incrementAndGet();
            MAP.remove(key);
        }
    }
}
