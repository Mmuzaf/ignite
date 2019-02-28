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
package org.apache.ignite.internal.processors.cache.persistence.preload;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.managers.communication.GridIoChannelListener;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloaderAssignments;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileTransferManager;
import org.apache.ignite.internal.processors.cache.persistence.file.meta.PartitionFileMetaInfo;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.nio.channel.IgniteSocketChannel;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.configuration.CacheConfiguration.DFLT_REBALANCE_TIMEOUT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.persistence.preload.GridCachePartitionUploadManager.persistenceRebalanceApplicable;
import static org.apache.ignite.internal.processors.cache.persistence.preload.GridCachePartitionUploadManager.rebalanceThreadTopic;
import static org.apache.ignite.internal.util.GridIntList.getAsIntList;

/**
 *
 */
public class GridCachePartitionDownloadManager extends GridCacheSharedManagerAdapter {
    /** */
    private static final int REBALANCE_TOPIC_IDX = 0;

    /** */
    private final ConcurrentMap<UUID, RebalanceDownloadFuture> futMap = new ConcurrentHashMap<>();

    /** */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** */
    private FilePageStoreManager filePageStore;

    /** */
    private volatile RebalanceDownloadFuture headFut = new RebalanceDownloadFuture();

    /**
     * @param ctx The kernal context.
     */
    public GridCachePartitionDownloadManager(GridKernalContext ctx) {
        assert CU.isPersistenceEnabled(ctx.config());
    }

    /**
     * @param assigns A generated cache assignments in a cut of cache group [grpId, [nodeId, parts]].
     * @param grp The corresponding to assignments cache group context.
     * @return {@code True} if cache might be rebalanced by sending cache partition files.
     */
    public static boolean rebalanceByPartitionSupports(CacheGroupContext grp, GridDhtPreloaderAssignments assigns) {
        if (assigns == null || assigns.isEmpty())
            return false;

        return grp.persistenceEnabled() && IgniteFeatures.allNodesSupports(assigns.keySet(),
            IgniteFeatures.CACHE_PARTITION_FILE_REBALANCE);
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        assert cctx.pageStore() instanceof FilePageStoreManager;

        filePageStore = (FilePageStoreManager)cctx.pageStore();

        if (persistenceRebalanceApplicable(cctx)) {
            // Register channel listeners for each rebalance thread.
            cctx.gridIO().addChannelListener(rebalanceThreadTopic(REBALANCE_TOPIC_IDX), new GridIoChannelListener() {
                @Override public void onChannelCreated(UUID nodeId, IgniteSocketChannel channel) {
                    if (lock.readLock().tryLock())
                        return;

                    try {
                        RebalanceDownloadFuture fut0 = futMap.get(nodeId);

                        if (fut0 == null || fut0.isComplete())
                            return;

                        onChannelCreated0(REBALANCE_TOPIC_IDX, nodeId, channel, fut0);
                    }
                    finally {
                        lock.readLock().unlock();
                    }
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        cctx.gridIO().removeChannelListener(rebalanceThreadTopic(REBALANCE_TOPIC_IDX), null);
    }

    /**
     * @param topicId The index of rebalance pool thread.
     * @param nodeId The remote node id.
     * @param channel A blocking socket channel to handle rebalance partitions.
     * @param rebFut The future of assignments handling.
     */
    private void onChannelCreated0(
        int topicId,
        UUID nodeId,
        IgniteSocketChannel channel,
        RebalanceDownloadFuture rebFut
    ) {
        assert rebFut.nodeId.equals(nodeId);

        U.log(log, "Handle channel created event [topicId=" + topicId + ", channel=" + channel + ']');

        FileTransferManager<PartitionFileMetaInfo> source = null;

        int totalParts = rebFut.nodeAssigns.values().stream()
            .mapToInt(GridIntList::size)
            .sum();

        AffinityTopologyVersion topVer = rebFut.topVer;
        Integer grpId = null;
        Integer partId = null;

        try {
            source = new FileTransferManager<>(cctx.kernalContext(), channel.channel());

            PartitionFileMetaInfo meta;

            for (int i = 0; i < totalParts; i++) {
                // Start processing original partition file.
                source.readMetaInto(meta = new PartitionFileMetaInfo());

                assert meta.getType() == 0 : meta;

                grpId = meta.getGrpId();
                partId = meta.getPartId();

                CacheGroupContext grp = cctx.cache().cacheGroup(grpId);
                AffinityAssignment aff = grp.affinity().cachedAffinity(topVer);

                if (aff.get(partId).contains(cctx.localNode())) {
                    GridDhtLocalPartition part = grp.topology().localPartition(partId);

                    assert part != null;

                    if (part.state() == MOVING) {
                        boolean reserved = part.reserve();

                        assert reserved : "Failed to reserve partition [igniteInstanceName=" +
                            cctx.igniteInstanceName() + ", grp=" + grp.cacheOrGroupName() + ", part=" + part + ']';

                        part.lock();

                        try {
                            FilePageStore store = (FilePageStore)filePageStore.getStore(grpId, partId);

                            File cfgFile = new File(store.getFileAbsolutePath());

                            // Skip the file header and first pageId with meta.
                            // Will restore meta pageId on merge delta file phase.
                            assert store.size() <= meta.getSize() : "Trim zero bytes from the end of partition";

                            source.readInto(cfgFile, store.headerSize() + store.getPageSize(), meta.getSize());

                            // Start processing delta file.
                            source.readMetaInto(meta = new PartitionFileMetaInfo());

                            assert meta.getType() == 1 : meta;

                            applyPartitionDeltaPages(source, store, meta.getSize());

                            rebFut.markProcessed(grpId, partId);

                            // Validate partition

                            // Rebuild indexes by partition

                            // Own partition
                        }
                        finally {
                            part.unlock();
                            part.release();
                        }
                    }
                    else {
                        if (log.isDebugEnabled()) {
                            log.debug("Skipping partition (state is not MOVING) " +
                                "[grpId=" + grpId + ", partId=" + partId + ", topicId=" + topicId +
                                ", nodeId=" + nodeId + ']');
                        }
                    }
                }
            }

            rebFut.onCompleteSuccess(true);
        }
        catch (IOException | IgniteCheckedException e) {
            U.error(log, "An error during receiving binary data from channel: " + channel, e);

            rebFut.onDone(new IgniteCheckedException("Error with downloading binary data from remote node " +
                "[grpId=" + String.valueOf(grpId) + ", partId=" + String.valueOf(partId) + ", topicId=" + topicId +
                ", nodeId=" + nodeId + ']', e));
        }
        finally {
            U.closeQuiet(source);
        }
    }

    /**
     * @param ftMgr The manager handles channel.
     * @param store Cache partition store.
     * @param size Expected size of bytes in channel.
     * @throws IgniteCheckedException If fails.
     */
    private void applyPartitionDeltaPages(
        FileTransferManager<PartitionFileMetaInfo> ftMgr,
        PageStore store,
        long size
    ) throws IgniteCheckedException {
        ByteBuffer pageBuff = ByteBuffer.allocate(store.getPageSize());

        long readed;
        long position = 0;

        while ((readed = ftMgr.readInto(pageBuff)) > 0 && position < size) {
            position += readed;

            pageBuff.flip();

            long pageId = PageIO.getPageId(pageBuff);
            long pageOffset = store.pageOffset(pageId);

            if (log.isDebugEnabled())
                log.debug("Page delta [pageId=" + pageId +
                    ", pageOffset=" + pageOffset +
                    ", partSize=" + store.size() +
                    ", skipped=" + (pageOffset >= store.size()) +
                    ", position=" + position +
                    ", size=" + size + ']');

            pageBuff.rewind();

            assert pageOffset < store.size();

            store.write(pageId, pageBuff, Integer.MAX_VALUE, false);

            pageBuff.clear();
        }
    }

    /**
     * @param assignsMap The map of cache groups assignments to process.
     * @return The map of cache assignments <tt>[group_order, [node, [group_id, partitions]]]</tt>
     */
    private NavigableMap<Integer, Map<ClusterNode, Map<Integer, GridIntList>>> sliceNodeCacheAssignments(
        Map<Integer, GridDhtPreloaderAssignments> assignsMap
    ) {
        NavigableMap<Integer, Map<ClusterNode, Map<Integer, GridIntList>>> result = new TreeMap<>();

        for (Map.Entry<Integer, GridDhtPreloaderAssignments> grpEntry : assignsMap.entrySet()) {
            int grpId = grpEntry.getKey();
            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            if (rebalanceByPartitionSupports(grp, grpEntry.getValue())) {
                int grpOrderNo = grp.config().getRebalanceOrder();

                result.putIfAbsent(grpOrderNo, new HashMap<>());

                for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> grpAssigns : grpEntry.getValue().entrySet()) {
                    ClusterNode node = grpAssigns.getKey();

                    result.get(grpOrderNo).putIfAbsent(node, new HashMap<>());

                    GridIntList intParts = getAsIntList(grpAssigns.getValue().partitions().fullSet());

                    if (!intParts.isEmpty())
                        result.get(grpOrderNo).get(node).putIfAbsent(grpId, intParts);
                }
            }
        }

        return result;
    }

    /**
     * @param assignsMap A map of cache assignments grouped by grpId.
     * @param force {@code true} if must cancel previous rebalance.
     * @param rebalanceId Current rebalance id.
     * @return Runnable to execute the chain.
     */
    public Runnable addNodeAssignments(
        Map<Integer, GridDhtPreloaderAssignments> assignsMap,
        AffinityTopologyVersion topVer,
        boolean force,
        int rebalanceId
    ) {
        // Start new rebalance session.
        final RebalanceDownloadFuture headFut0 = headFut;

        if (!headFut0.isDone())
            headFut0.cancel();

        NavigableMap<Integer, Map<ClusterNode, Map<Integer, GridIntList>>> nodeOrderAssignsMap =
            sliceNodeCacheAssignments(assignsMap);

        // TODO Start eviction.

        RebalanceDownloadFuture rqFut = null;
        Runnable rq = null;

        for (Map<ClusterNode, Map<Integer, GridIntList>> descNodeMap : nodeOrderAssignsMap.descendingMap().values()) {
            for (Map.Entry<ClusterNode, Map<Integer, GridIntList>> assignEntry : descNodeMap.entrySet()) {
                RebalanceDownloadFuture rebFut = new RebalanceDownloadFuture(assignEntry.getKey().id(), rebalanceId,
                    assignEntry.getValue(), topVer);

                final Runnable nextRq0 = rq;

                if (rqFut != null) {
                    rqFut.listen(f -> {
                        try {
                            if (f.get()) // Not cancelled.
                                nextRq0.run();
                        }
                        catch (IgniteCheckedException e) {
                            rebFut.onDone(e);

                            U.error(log, "Cache partitions rebalance failed", e);
                        }
                    });
                }
                else {
                    // The first seen rebalance fut.
                    headFut = rebFut;
                }

                rq = requestNodePartitions(assignEntry.getKey(), REBALANCE_TOPIC_IDX, rebFut);
                rqFut = rebFut;
            }
        }

        return rq;
    }

    /**
     * @param node Clustre node to send inital demand message to.
     * @param topicId Rebalacne thread topic.
     * @param rebFut The future to handle demand request.
     */
    private Runnable requestNodePartitions(
        ClusterNode node,
        int topicId,
        RebalanceDownloadFuture rebFut
    ) {
        return new Runnable() {
            @Override public void run() {
                if (rebFut.isComplete())
                    return;

                try {
                    GridPartitionsCopyDemandMessage msg0 = new GridPartitionsCopyDemandMessage(rebFut.rebalanceId,
                        rebFut.topVer, rebFut.nodeAssigns);

                    futMap.put(node.id(), rebFut);

                    cctx.gridIO().sendOrderedMessage(node, rebalanceThreadTopic(topicId),
                        msg0, SYSTEM_POOL, DFLT_REBALANCE_TIMEOUT, false);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Error sending request for demanded cache partitions", e);

                    rebFut.onDone(e);

                    futMap.remove(node.id());
                }
            }
        };
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCachePartitionDownloadManager.class, this);
    }

    /** */
    private static class RebalanceDownloadFuture extends GridFutureAdapter<Boolean> {
        /** */
        private UUID nodeId;

        /** */
        private int rebalanceId;

        /** */
        private Map<Integer, GridIntList> nodeAssigns;

        /** */
        private AffinityTopologyVersion topVer;

        /** */
        private Map<Integer, GridIntList> remaining;

        /**
         * Default constructor for the dummy future.
         */
        public RebalanceDownloadFuture() {
            onDone();
        }

        /**
         * @param nodeId The remote nodeId.
         * @param nodeAssigns Map of assignments to request from remote.
         */
        public RebalanceDownloadFuture(
            UUID nodeId,
            int rebalanceId,
            Map<Integer, GridIntList> nodeAssigns,
            AffinityTopologyVersion topVer
        ) {
            this.nodeId = nodeId;
            this.rebalanceId = rebalanceId;
            this.nodeAssigns = nodeAssigns;
            this.topVer = topVer;

            this.remaining = U.newHashMap(nodeAssigns.size());

            for (Map.Entry<Integer, GridIntList> grpPartEntry : nodeAssigns.entrySet())
                remaining.putIfAbsent(grpPartEntry.getKey(), grpPartEntry.getValue().copy());
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            return onCancelled();
        }

        /**
         * @param res Result to set.
         */
        public synchronized void onCompleteSuccess(Boolean res) {
            assert remaining.isEmpty();

            onDone(res);
        }

        /**
         * @return {@code True} if current future cannot be processed.
         */
        public boolean isComplete() {
            return isCancelled() || isFailed() || isDone();
        }

        /**
         * @param grpId Cache group id to search.
         * @param partId Cache partition to remove;
         * @throws IgniteCheckedException If fails.
         */
        public synchronized void markProcessed(int grpId, int partId) throws IgniteCheckedException {
            GridIntList parts = remaining.get(grpId);

            if (parts == null)
                throw new IgniteCheckedException("Partition index incorrect [grpId=" + grpId + ", partId=" + partId + ']');

            int partIdx = parts.removeValue(0, partId);

            assert partIdx >= 0;

            if (parts.isEmpty())
                remaining.remove(grpId);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RebalanceDownloadFuture.class, this);
        }
    }
}
