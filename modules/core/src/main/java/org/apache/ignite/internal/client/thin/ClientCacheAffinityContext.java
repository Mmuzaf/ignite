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

package org.apache.ignite.internal.client.thin;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.GridConcurrentHashSet;

/**
 * Client cache partition awareness context.
 */
public class ClientCacheAffinityContext {
    /** Default key mapper. */
    private static final Function<Integer, ToIntFunction<Object>> DEFAULT_KEY_MAPPER = parts -> null;

    /** Binary data processor. */
    private final IgniteBinary binary;

    /** Factory for each cache to produce key to partition mappers. */
    private final Map<Integer, Function<Integer, ToIntFunction<Object>>> cacheKeyMapperFactoryMap = new ConcurrentHashMap<>();

    /** Contains last topology version and known nodes of this version. */
    private final AtomicReference<TopologyNodes> lastTop = new AtomicReference<>();

    /** Current affinity mapping. */
    private volatile ClientCacheAffinityMapping affinityMapping;

    /** Cache IDs, which should be included to the next affinity mapping request. */
    private final Set<Integer> pendingCacheIds = new GridConcurrentHashSet<>();

    /**
     * @param binary Binary data processor.
     */
    public ClientCacheAffinityContext(IgniteBinary binary) {
        this.binary = binary;
    }

    /**
     * Update topology version if it's greater than current version and store nodes for last topology.
     *
     * @param topVer Topology version.
     * @param nodeId Node id.
     * @return {@code True} if last topology was updated to the new version.
     */
    public boolean updateLastTopologyVersion(AffinityTopologyVersion topVer, UUID nodeId) {
        while (true) {
            TopologyNodes lastTop = this.lastTop.get();

            if (lastTop == null || topVer.compareTo(lastTop.topVer) > 0) {
                if (this.lastTop.compareAndSet(lastTop, new TopologyNodes(topVer, nodeId)))
                    return true;
            }
            else if (topVer.equals(lastTop.topVer)) {
                lastTop.nodes.add(nodeId);

                return false;
            }
            else
                return false;
        }
    }

    /**
     * Is affinity update required for given cache.
     *
     * @param cacheId Cache id.
     */
    public boolean affinityUpdateRequired(int cacheId) {
        TopologyNodes top = lastTop.get();

        if (top == null) { // Don't know current topology.
            pendingCacheIds.add(cacheId);

            return false;
        }

        ClientCacheAffinityMapping mapping = affinityMapping;

        if (mapping == null) {
            pendingCacheIds.add(cacheId);

            return true;
        }

        if (top.topVer.compareTo(mapping.topologyVersion()) > 0) {
            pendingCacheIds.add(cacheId);

            return true;
        }

        if (mapping.cacheIds().contains(cacheId))
            return false;
        else {
            pendingCacheIds.add(cacheId);

            return true;
        }
    }

    /**
     * @param ch Payload output channel.
     */
    public void writePartitionsUpdateRequest(PayloadOutputChannel ch) {
        ClientCacheAffinityMapping.writeRequest(ch, pendingCacheIds, cacheKeyMapperFactoryMap::get);
    }

    /**
     * @param ch Payload input channel.
     */
    public synchronized boolean readPartitionsUpdateResponse(PayloadInputChannel ch) {
        if (lastTop.get() == null)
            return false;

        ClientCacheAffinityMapping newMapping = ClientCacheAffinityMapping.readResponse(ch,
            cacheId -> cacheKeyMapperFactoryMap.getOrDefault(cacheId, DEFAULT_KEY_MAPPER));

        ClientCacheAffinityMapping oldMapping = affinityMapping;

        if (oldMapping == null || newMapping.topologyVersion().compareTo(oldMapping.topologyVersion()) > 0) {
            affinityMapping = newMapping;

            if (oldMapping != null)
                pendingCacheIds.addAll(oldMapping.cacheIds());

            pendingCacheIds.removeAll(newMapping.cacheIds());

            return true;
        }

        if (newMapping.topologyVersion().equals(oldMapping.topologyVersion())) {
            affinityMapping = ClientCacheAffinityMapping.merge(oldMapping, newMapping);

            pendingCacheIds.removeAll(newMapping.cacheIds());

            return true;
        }

        // Obsolete mapping.
        return true;
    }

    /**
     * Gets last topology information.
     */
    public TopologyNodes lastTopology() {
        return lastTop.get();
    }

    /**
     * Resets affinity context.
     *
     * @param top Topology which triggers reset.
     */
    public synchronized void reset(TopologyNodes top) {
        if (lastTop.compareAndSet(top, null)) {
            affinityMapping = null;

            pendingCacheIds.clear();
        }
    }

    /**
     * Calculates affinity node for given cache and key.
     *
     * @param cacheId Cache ID.
     * @param key Key.
     * @return Affinity node id or {@code null} if affinity node can't be determined for given cache and key.
     */
    public UUID affinityNode(int cacheId, Object key) {
        ClientCacheAffinityMapping mapping = currentMapping();

        return mapping == null ? null : mapping.affinityNode(binary, cacheId, key);
    }

    /**
     * Calculates affinity node for given cache and partition.
     *
     * @param cacheId Cache ID.
     * @param part Partition.
     * @return Affinity node id or {@code null} if affinity node can't be determined for given cache and partition.
     */
    public UUID affinityNode(int cacheId, int part) {
        ClientCacheAffinityMapping mapping = currentMapping();

        return mapping == null ? null : mapping.affinityNode(cacheId, part);
    }

    /**
     * Current affinity mapping.
     */
    private ClientCacheAffinityMapping currentMapping() {
        TopologyNodes top = lastTop.get();

        if (top == null)
            return null;

        ClientCacheAffinityMapping mapping = affinityMapping;

        if (mapping == null)
            return null;

        if (top.topVer.compareTo(mapping.topologyVersion()) > 0)
            return null;

        return mapping;
    }

    /**
     * @param cacheId Cache id.
     * @param factory Key mapper factory.
     */
    public void addKeyMapperFactory(int cacheId, Function<Integer, ToIntFunction<Object>> factory) {
        cacheKeyMapperFactoryMap.putIfAbsent(cacheId, factory);
    }

    /**
     * Holder for list of nodes for topology version.
     */
    static class TopologyNodes {
        /** Topology version. */
        private final AffinityTopologyVersion topVer;

        /** Nodes. */
        private final Collection<UUID> nodes = new ConcurrentLinkedQueue<>();

        /**
         * @param topVer Topology version.
         * @param nodeId Node id.
         */
        private TopologyNodes(AffinityTopologyVersion topVer, UUID nodeId) {
            this.topVer = topVer;

            nodes.add(nodeId);
        }

        /**
         * Gets nodes of this topology.
         */
        public Iterable<UUID> nodes() {
            return Collections.unmodifiableCollection(nodes);
        }
    }
}
