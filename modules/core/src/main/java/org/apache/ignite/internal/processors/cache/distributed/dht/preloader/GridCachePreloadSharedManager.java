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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotListener;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 * todo naming
 * GridPartitionFilePreloader
 * GridCachePartitionFilePreloader
 * GridFilePreloader
 * GridPartitionPreloader
 * GridSnapshotFilePreloader
 */
public class GridCachePreloadSharedManager extends GridCacheSharedManagerAdapter {
    /** */
    public static final String REBALANCE_CP_REASON = "Rebalance has been scheduled [grps=%s]";

    /** */
    private static final Runnable NO_OP = () -> {};

    /** todo */
    private static final boolean presistenceRebalanceEnabled = IgniteSystemProperties.getBoolean(
        IgniteSystemProperties.IGNITE_PERSISTENCE_REBALANCE_ENABLED, false);

    /** todo add default threshold  */
    private static final long MIN_PART_SIZE_FOR_FILE_REBALANCING = IgniteSystemProperties.getLong(
        IGNITE_PDS_WAL_REBALANCE_THRESHOLD, DFLT_IGNITE_PDS_WAL_REBALANCE_THRESHOLD);

    /** */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Checkpoint listener. */
    private final CheckpointListener cpLsnr = new CheckpointListener();

    /** */
    private volatile FileRebalanceFuture fileRebalanceFut = new FileRebalanceFuture();

    /**
     * @param ktx Kernal context.
     */
    public GridCachePreloadSharedManager(GridKernalContext ktx) {
        assert CU.isPersistenceEnabled(ktx.config()) :
            "Persistence must be enabled to preload any of cache partition files";
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        ((GridCacheDatabaseSharedManager)cctx.database()).addCheckpointListener(cpLsnr);

        cctx.snapshotMgr().addSnapshotListener(new PartitionSnapshotListener());
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        lock.writeLock().lock();

        try {
            ((GridCacheDatabaseSharedManager)cctx.database()).removeCheckpointListener(cpLsnr);

            fileRebalanceFut.cancel();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    // todo the result assignment should be equal to generate assignments
    public void onExchangeDone(GridDhtPartitionsExchangeFuture exchFut) {
        try {
            assert exchFut != null;

            GridDhtPartitionExchangeId exchId = exchFut.exchangeId();

            if (cctx.exchange().hasPendingExchange()) {
                if (log.isDebugEnabled())
                    log.debug("Skipping rebalancing initializa exchange worker has pending exchange: " + exchId);

                return;
            }

            AffinityTopologyVersion topVer = exchFut.topologyVersion();

            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                if (!grp.dataRegion().config().isPersistenceEnabled() || CU.isUtilityCache(grp.cacheOrGroupName()))
                    continue;

                int partitions = grp.affinity().partitions();

                AffinityAssignment aff = grp.affinity().readyAffinity(topVer);

                assert aff != null;

                for (int p = 0; p < partitions; p++) {
                    if (aff.get(p).contains(cctx.localNode())) {
                        GridDhtLocalPartition part = grp.topology().localPartition(p);

                        if (part.state() == OWNING)
                            continue;

                        assert part.state() == MOVING : "Unexpected state [cache=" + grp.cacheOrGroupName() +
                            ", p=" + p + "state=" + part.state() + "]";

                        // Should have partition file supplier to start file rebalance.
                        if (exchFut.partitionFileSupplier(grp.groupId(), p) != null) {
                            part.readOnly(true);
                            part.dataStore().reinit();
                        }
//                        else
//                            part.readOnly(false);
                    }
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public void onTopologyChanged(GridDhtPartitionsExchangeFuture exchFut) {
        if (log.isInfoEnabled())
            log.info("Topology changed - canceling file rebalance.");

        fileRebalanceFut.cancel();
    }

    /**
     * This method initiates new file rebalance process from given {@code assignments} by creating new file
     * rebalance future based on them. Cancels previous file rebalance future and sends rebalance started event (todo).
     * In case of delayed rebalance method schedules the new one with configured delay based on {@code lastExchangeFut}.
     *
     * @param assignsMap A map of cache assignments grouped by grpId.
     * @param force {@code true} if must cancel previous rebalance.
     * @param rebalanceId Current rebalance id.
     * @return Runnable to execute the chain.
     */
    public Runnable addNodeAssignments(
        Map<Integer, GridDhtPreloaderAssignments> assignsMap,
        AffinityTopologyVersion topVer,
        boolean force,
        long rebalanceId) {
        NavigableMap<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> nodeOrderAssignsMap =
            sliceNodeCacheAssignments(assignsMap);

        if (nodeOrderAssignsMap.isEmpty())
            return NO_OP;

        if (!cctx.kernalContext().grid().isRebalanceEnabled()) {
            if (log.isDebugEnabled())
                log.debug("Cancel partition file demand because rebalance disabled on current node.");

            return NO_OP;
        }

        // Start new rebalance session.
        FileRebalanceFuture rebFut = fileRebalanceFut;

        lock.writeLock().lock();

        try {
            if (!rebFut.isDone())
                rebFut.cancel();

            fileRebalanceFut = rebFut = new FileRebalanceFuture(cpLsnr, assignsMap, topVer, cctx, log);

            FileRebalanceNodeFuture rqFut = null;
            Runnable rq = NO_OP;

            if (log.isInfoEnabled())
                log.info("Prepare the chain to demand assignments: " + nodeOrderAssignsMap);

            for (Map.Entry<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> entry : nodeOrderAssignsMap.descendingMap().entrySet()) {
                Map<ClusterNode, Map<Integer, Set<Integer>>> descNodeMap = entry.getValue();

                int order = entry.getKey();

                for (Map.Entry<ClusterNode, Map<Integer, Set<Integer>>> assignEntry : descNodeMap.entrySet()) {
                    FileRebalanceNodeFuture fut = new FileRebalanceNodeFuture(cctx, fileRebalanceFut, log, assignEntry.getKey(),
                        order, rebalanceId, assignEntry.getValue(), topVer);

                    rebFut.add(order, fut);

                    final Runnable nextRq0 = rq;
                    final FileRebalanceNodeFuture rqFut0 = rqFut;

//                }
//                    else {

                    if (rqFut0 != null) {
                        // xxxxFut = xxxFut; // The first seen rebalance node.
                        fut.listen(f -> {
                            try {
                                if (log.isDebugEnabled())
                                    log.debug("Running next task, last future result is " + f.get());

                                if (f.get()) // Not cancelled.
                                    nextRq0.run();
                                // todo check how this chain is cancelling
                            }
                            catch (IgniteCheckedException e) {
                                rqFut0.onDone(e);
                            }
                        });
                    }

                    rq = fut::requestPartitions;
                    rqFut = fut;
                }
            }

            cctx.kernalContext().getSystemExecutorService().submit(rebFut::clearPartitions);

            rebFut.listen(new IgniteInClosureX<IgniteInternalFuture<Boolean>>() {
                @Override public void applyx(IgniteInternalFuture<Boolean> fut0) throws IgniteCheckedException {
                    if (fut0.isCancelled()) {
                        log.info("File rebalance canceled [topVer=" + topVer + "]");

                        return;
                    }

                    if (log.isInfoEnabled())
                        log.info("The final persistence rebalance is done [result=" + fut0.get() + ']');
                }
            });

            return rq;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param assignsMap The map of cache groups assignments to process.
     * @return The map of cache assignments <tt>[group_order, [node, [group_id, partitions]]]</tt>
     */
    private NavigableMap<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> sliceNodeCacheAssignments(
        Map<Integer, GridDhtPreloaderAssignments> assignsMap) {
        NavigableMap<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> result = new TreeMap<>();

        for (Map.Entry<Integer, GridDhtPreloaderAssignments> grpEntry : assignsMap.entrySet()) {
            int grpId = grpEntry.getKey();

            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            GridDhtPreloaderAssignments assigns = grpEntry.getValue();

            if (fileRebalanceRequired(grp, assigns)) {
                int grpOrderNo = grp.config().getRebalanceOrder();

                result.putIfAbsent(grpOrderNo, new HashMap<>());

                for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> grpAssigns : assigns.entrySet()) {
                    ClusterNode node = grpAssigns.getKey();

                    result.get(grpOrderNo).putIfAbsent(node, new HashMap<>());

                    result.get(grpOrderNo)
                        .get(node)
                        .putIfAbsent(grpId,
                            grpAssigns.getValue()
                                .partitions()
                                .fullSet());
                }
            }
        }

        return result;
    }

    /**
     * todo access
     * @param fut The future to check.
     * @return <tt>true</tt> if future can be processed.
     */
    boolean staleFuture(FileRebalanceNodeFuture fut) {
        return fut == null || fut.isCancelled() || fut.isFailed() || fut.isDone() || topologyChanged(fut);
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param assignments Preloading assignments.
     * @return {@code True} if cache must be rebalanced by sending files.
     */
    public boolean fileRebalanceRequired(CacheGroupContext grp, GridDhtPreloaderAssignments assignments) {
        if (!fileRebalanceRequired(grp, assignments.keySet()))
            return false;

        for (GridDhtPartitionDemandMessage msg : assignments.values()) {
            if (msg.partitions().hasHistorical())
                return false;
        }

        return true;
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param nodes Preloading assignments.
     * @return {@code True} if cache must be rebalanced by sending files.
     */
    public boolean fileRebalanceRequired(CacheGroupContext grp, Collection<ClusterNode> nodes) {
        return fileRebalanceSupported(grp, nodes) &&
            grp.config().getRebalanceDelay() != -1 &&
            grp.config().getRebalanceMode() != CacheRebalanceMode.NONE;
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param nodes Preloading assignments.
     * @return {@code True} if cache might be rebalanced by sending cache partition files.
     */
    public boolean fileRebalanceSupported(CacheGroupContext grp, Collection<ClusterNode> nodes) {
        if (nodes == null || nodes.isEmpty())
            return false;

        // Do not rebalance system cache with files as they are not exists.
        if (grp.groupId() == CU.cacheId(UTILITY_CACHE_NAME))
            return false;

        if (grp.mvccEnabled())
            return false;

        Map<Integer, Long> globalSizes = grp.topology().globalPartSizes();

        if (globalSizes != null && !globalSizes.isEmpty()) {
            boolean required = false;

            // enabling file rebalancing only when we have at least one big enough partition
            for (Long partSize : globalSizes.values()) {
                if (partSize >= MIN_PART_SIZE_FOR_FILE_REBALANCING) {
                    required = true;

                    break;
                }
            }

            if (!required)
                return false;
        }

        if (!presistenceRebalanceEnabled ||
            !grp.persistenceEnabled() ||
            !IgniteFeatures.allNodesSupports(nodes, IgniteFeatures.CACHE_PARTITION_FILE_REBALANCE))
            return false;

//        for (GridDhtPartitionDemandMessage msg : assignments.values()) {
//            if (msg.partitions().hasHistorical())
//                return false;
//        }

        return true;
    }

    /**
     * Restore partition on new file. Partition should be completely destroyed before restore it with new file.
     *
     * @param grpId Group id.
     * @param partId Partition number.
     * @param src New partition file on the same filesystem.
     * @param fut
     * @return Future that will be completed when partition will be fully re-initialized. The future result is the HWM
     * value of update counter in read-only partition.
     * @throws IgniteCheckedException If file store for specified partition doesn't exists or partition file cannot be
     * moved.
     */
    public IgniteInternalFuture<T2<Long, Long>> restorePartition(int grpId, int partId, File src,
        FileRebalanceNodeFuture fut) throws IgniteCheckedException {
        if (staleFuture(fut))
            return null;

        FilePageStore pageStore = ((FilePageStore)((FilePageStoreManager)cctx.pageStore()).getStore(grpId, partId));

        try {
            File dest = new File(pageStore.getFileAbsolutePath());

            if (log.isDebugEnabled()) {
                log.debug("Moving downloaded partition file [from=" + src +
                    " , to=" + dest + " , size=" + src.length() + "]");
            }

            assert !cctx.pageStore().exists(grpId, partId) : "Partition file exists [cache=" +
                cctx.cache().cacheGroup(grpId).cacheOrGroupName() + ", p=" + partId + "]";

            Files.move(src.toPath(), dest.toPath());
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Unable to move file [source=" + src +
                ", target=" + pageStore.getFileAbsolutePath() + "]", e);
        }

        GridDhtLocalPartition part = cctx.cache().cacheGroup(grpId).topology().localPartition(partId);

        part.dataStore().store(false).reinit();

        GridFutureAdapter<T2<Long, Long>> endFut = new GridFutureAdapter<>();

        cpLsnr.schedule(() -> {
            if (staleFuture(fut))
                return;

            // Save current update counter.
            PartitionUpdateCounter maxCntr = part.dataStore().partUpdateCounter();

            assert maxCntr != null;

            part.readOnly(false);

            // Clear all on heap entries.
            // todo something smarter
            // todo check on large partition
            part.entriesMap(null).map.clear();

            PartitionUpdateCounter minCntr = part.dataStore().partUpdateCounter();

            assert minCntr != null;
            // todo check empty partition
            assert minCntr.get() != 0 : "grpId=" + grpId + ", p=" + partId + ", fullSize=" + part.dataStore().fullSize();

            AffinityTopologyVersion infinTopVer = new AffinityTopologyVersion(Long.MAX_VALUE, 0);

            IgniteInternalFuture<?> partReleaseFut = cctx.partitionReleaseFuture(infinTopVer);

            // Operations that are in progress now will be lost and should be included in historical rebalancing.
            // These operations can update the old update counter or the new update counter, so the maximum applied
            // counter is used after all updates are completed.
            // todo Consistency check fails sometimes for ATOMIC cache.
            partReleaseFut.listen(c ->
                endFut.onDone(
                    new T2<>(minCntr.get(), Math.max(maxCntr.highestAppliedCounter(), minCntr.highestAppliedCounter()))
                )
            );
        });

        return endFut;
    }

    /**
     * @param fut Future.
     * @return {@code True} if rebalance topology version changed by exchange thread or force
     * reassing exchange occurs, see {@link RebalanceReassignExchangeTask} for details.
     */
    private boolean topologyChanged(FileRebalanceNodeFuture fut) {
        return !cctx.exchange().rebalanceTopologyVersion().equals(fut.topologyVersion());
        // todo || fut != rebalanceFut; // Same topology, but dummy exchange forced because of missing partitions.
    }

    /** */
    public static class CheckpointListener implements DbCheckpointListener {
        /** Queue. */
        private final ConcurrentLinkedQueue<CheckpointTask> queue = new ConcurrentLinkedQueue<>();

        /** {@inheritDoc} */
        @Override public void onMarkCheckpointBegin(Context ctx) {
            Runnable r;

            while ((r = queue.poll()) != null)
                r.run();
        }

        /** {@inheritDoc} */
        @Override public void onCheckpointBegin(Context ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void beforeCheckpointBegin(Context ctx) {
            // No-op.
        }

        /** */
        public void cancelAll() {
            ArrayList<CheckpointTask> tasks = new ArrayList<>(queue);

            queue.clear();

            for (CheckpointTask task : tasks)
                task.fut.onDone();
        }

        public IgniteInternalFuture<Void> schedule(final Runnable task) {
            return schedule(new CheckpointTask<>(() -> {
                task.run();

                return null;
            }));
        }

//        public <R> IgniteInternalFuture<R> schedule(final Callable<R> task) {
//            return schedule(new CheckpointTask<>(task));
//        }

        private <R> IgniteInternalFuture<R> schedule(CheckpointTask<R> task) {
            queue.offer(task);

            return task.fut;
        }

        /** */
        private static class CheckpointTask<R> implements Runnable {
            /** */
            final GridFutureAdapter<R> fut = new GridFutureAdapter<>();

            /** */
            final Callable<R> task;

            /** */
            CheckpointTask(Callable<R> task) {
                this.task = task;
            }

            /** {@inheritDoc} */
            @Override public void run() {
                try {
                    fut.onDone(task.call());
                }
                catch (Exception e) {
                    fut.onDone(e);
                }
            }
        }
    }

    /**
     * Partition snapshot listener.
     */
    private class PartitionSnapshotListener implements SnapshotListener {
        /** {@inheritDoc} */
        @Override public void onPartition(UUID nodeId, String snpName, File file, int grpId, int partId) {
            FileRebalanceNodeFuture fut = fileRebalanceFut.nodeRoutine(grpId, nodeId);

            if (staleFuture(fut) || !snpName.equals(fut.snapshotName())) {
                if (log.isDebugEnabled())
                    log.debug("Cancel partitions download due to stale rebalancing future [current snapshot=" + snpName + ", fut=" + fut);

                // todo
                file.delete();

                return;
//                // todo how cancel current download
//                throw new IgniteException("Cancel partitions download due to stale rebalancing future.");
            }

            try {
                fileRebalanceFut.awaitCleanupIfNeeded(grpId);

                IgniteInternalFuture<T2<Long, Long>> restoreFut = restorePartition(grpId, partId, file, fut);

                // todo
                if (topologyChanged(fut)) {
                    log.info("Cancel partitions download due to topology changes.");

                    file.delete();

                    fut.cancel();

                    throw new IgniteException("Cancel partitions download due to topology changes.");
                }

                restoreFut.listen(f -> {
                    try {
                        T2<Long, Long> cntrs = f.get();

                        assert cntrs != null;

                        cctx.kernalContext().closure().runLocalSafe(() -> {
                            fut.onPartitionRestored(grpId, partId, cntrs.get1(), cntrs.get2());
                        });
                    }
                    catch (IgniteCheckedException e) {
                        log.error("Unable to restore partition snapshot [cache=" +
                            cctx.cache().cacheGroup(grpId) + ", p=" + partId, e);

                        fut.onDone(e);
                    }
                });
            }
            catch (IgniteCheckedException e) {
                log.error("Unable to handle partition snapshot", e);

                fut.onDone(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void onEnd(UUID rmtNodeId, String snpName) {
            // No-op.
            // todo add assertion
        }

        /** {@inheritDoc} */
        @Override public void onException(UUID rmtNodeId, String snpName, Throwable t) {
            if (t instanceof ClusterTopologyCheckedException) {
                if (log.isDebugEnabled())
                    log.debug("Snapshot canceled (topology changed): " + snpName);

//                fileRebalanceFut.cancel();

                return;
            }

            log.error("Unable to create remote snapshot: " + snpName, t);

//            fileRebalanceFut.onDone(t);
        }
    }
}
