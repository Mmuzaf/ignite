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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.MarshallerMappingWriter;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.communication.TransmissionCancelledException;
import org.apache.ignite.internal.managers.communication.TransmissionHandler;
import org.apache.ignite.internal.managers.communication.TransmissionMeta;
import org.apache.ignite.internal.managers.communication.TransmissionPolicy;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cacheobject.BinaryTypeWriter;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.lang.IgniteThrowableSupplier;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static java.nio.file.StandardOpenOption.READ;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.IgniteFeatures.PERSISTENCE_CACHE_SNAPSHOT;
import static org.apache.ignite.internal.IgniteFeatures.nodeSupports;
import static org.apache.ignite.internal.MarshallerContextImpl.addPlatformMappings;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.MAX_PARTITION_ID;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFileName;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor.DB_DEFAULT_FOLDER;
import static org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId.getFlagByPartId;

/** */
public class IgniteSnapshotManager extends GridCacheSharedManagerAdapter {
    /** File with delta pages suffix. */
    public static final String DELTA_SUFFIX = ".delta";

    /** File name template consists of delta pages. */
    public static final String PART_DELTA_TEMPLATE = PART_FILE_TEMPLATE + DELTA_SUFFIX;

    /** File name template for index delta pages. */
    public static final String INDEX_DELTA_NAME = INDEX_FILE_NAME + DELTA_SUFFIX;

    /** The reason of checkpoint start for needs of snapshot. */
    public static final String SNAPSHOT_CP_REASON = "Wakeup for checkpoint to take snapshot [name=%s]";

    /** Default snapshot directory for loading remote snapshots. */
    public static final String DFLT_SNAPSHOT_WORK_DIRECTORY = "snp";

    /** Timeout in milliseconsd for snapshot operations. */
    public static final long DFLT_SNAPSHOT_TIMEOUT = 15_000L;

    /** Prefix for snapshot threads. */
    private static final String SNAPSHOT_RUNNER_THREAD_PREFIX = "snapshot-runner";

    /** Total number of thread to perform local snapshot. */
    private static final int SNAPSHOT_THREAD_POOL_SIZE = 4;

    /** Default snapshot topic to receive snapshots from remote node. */
    private static final Object DFLT_INITIAL_SNAPSHOT_TOPIC = GridTopic.TOPIC_SNAPSHOT.topic("rmt_snp");

    /** File transmission parameter of cache group id. */
    private static final String SNP_GRP_ID_PARAM = "grpId";

    /** File transmission parameter of cache partition id. */
    private static final String SNP_PART_ID_PARAM = "partId";

    /** File transmission parameter of node-sender directory path with its consistentId (e.g. db/IgniteNode0). */
    private static final String SNP_DB_NODE_PATH_PARAM = "dbNodePath";

    /** File transmission parameter of a cache directory with is currently sends its partitions. */
    private static final String SNP_CACHE_DIR_NAME_PARAM = "cacheDirName";

    /** Snapshot parameter name for a file transmission. */
    private static final String SNP_NAME_PARAM = "snpName";

    /** Map of registered cache snapshot processes and their corresponding contexts. */
    private final ConcurrentMap<String, SnapshotTask> locSnpTasks = new ConcurrentHashMap<>();

    /** Lock to protect the resources is used. */
    private final GridBusyLock busyLock = new GridBusyLock();

    /** Requested snapshot from remote node. */
    private final AtomicReference<RemoteSnapshotFuture> rmtSnpReq = new AtomicReference<>();

    /** Main snapshot directory to save created snapshots. */
    private File locSnpDir;

    /**
     * Working directory for loaded snapshots from the remote nodes and storing
     * temporary partition delta-files of locally started snapshot process.
     */
    private File tmpWorkDir;

    /** Factory to working with delta as file storage. */
    private volatile FileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /** Factory to create page store for restore. */
    private volatile BiFunction<Integer, Boolean, FilePageStoreFactory> storeFactory;

    /** Snapshot thread pool to perform local partition snapshots. */
    private ExecutorService snpRunner;

    /** System discovery message listener. */
    private DiscoveryEventListener discoLsnr;

    /** Snapshot listener on created snapshots. */
    private volatile SnapshotListener snpLsnr;

    /** Database manager for enabled persistence. */
    private GridCacheDatabaseSharedManager dbMgr;

    /** Configured data storage page size. */
    private int pageSize;

    /**
     * @param ctx Kernal context.
     */
    public IgniteSnapshotManager(GridKernalContext ctx) {
        // No-op.
    }

    /**
     * @param snapshotCacheDir Snapshot directory to store files.
     * @param partId Cache partition identifier.
     * @return A file representation.
     */
    public static File getPartionDeltaFile(File snapshotCacheDir, int partId) {
        return new File(snapshotCacheDir, getPartitionDeltaFileName(partId));
    }

    /**
     * @param partId Partitoin id.
     * @return File name of delta partition pages.
     */
    public static String getPartitionDeltaFileName(int partId) {
        assert partId <= MAX_PARTITION_ID || partId == INDEX_PARTITION;

        return partId == INDEX_PARTITION ? INDEX_DELTA_NAME : String.format(PART_DELTA_TEMPLATE, partId);
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        GridKernalContext kctx = cctx.kernalContext();

        if (kctx.clientNode())
            return;

        if (!CU.isPersistenceEnabled(cctx.kernalContext().config()))
            return;

        DataStorageConfiguration dcfg = kctx.config().getDataStorageConfiguration();

        pageSize = dcfg.getPageSize();

        assert pageSize > 0;

        snpRunner = new IgniteThreadPoolExecutor(
            SNAPSHOT_RUNNER_THREAD_PREFIX,
            cctx.igniteInstanceName(),
            SNAPSHOT_THREAD_POOL_SIZE,
            SNAPSHOT_THREAD_POOL_SIZE,
            30_000,
            new LinkedBlockingQueue<>(),
            SYSTEM_POOL,
            (t, e) -> kctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e)));

        assert cctx.pageStore() instanceof FilePageStoreManager;

        FilePageStoreManager storeMgr = (FilePageStoreManager)cctx.pageStore();

        locSnpDir = U.resolveWorkDirectory(kctx.config().getWorkDirectory(), dcfg.getLocalSnapshotPath(), false);
        tmpWorkDir = Paths.get(storeMgr.workDir().getAbsolutePath(), DFLT_SNAPSHOT_WORK_DIRECTORY).toFile();

        U.ensureDirectory(locSnpDir, "local snapshots directory", log);
        U.ensureDirectory(tmpWorkDir, "work directory for snapshots creation", log);

        storeFactory = storeMgr::getPageStoreFactory;
        dbMgr = (GridCacheDatabaseSharedManager)cctx.database();

        // Receive remote snapshots requests.
        cctx.gridIO().addMessageListener(DFLT_INITIAL_SNAPSHOT_TOPIC, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (!busyLock.enterBusy())
                    return;

                try {
                    if (msg instanceof SnapshotRequestMessage) {
                        SnapshotRequestMessage reqMsg0 = (SnapshotRequestMessage)msg;
                        String snpName = reqMsg0.snapshotName();
                        GridCacheSharedContext cctx0 = cctx;

                        try {
                            SnapshotTask task;

                            synchronized (rmtSnpReq) {
                                IgniteInternalFuture<Boolean> snpResp = snapshotRemoteRequest(nodeId);

                                if (snpResp != null) {
                                    // Task should also be removed from local map.
                                    snpResp.cancel();

                                    log.info("Snapshot request has been cancelled due to another request recevied " +
                                        "[prevSnpResp=" + snpResp + ", msg0=" + reqMsg0 + ']');
                                }

                                task = putSnapshotTask(snpName,
                                    nodeId,
                                    reqMsg0.parts(),
                                    new SerialExecutor(cctx0.kernalContext()
                                        .pools()
                                        .poolForPolicy(plc)),
                                    remoteSnapshotSender(snpName,
                                        nodeId));
                            }

                            task.run();
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to proccess request of creating a snapshot " +
                                "[from=" + nodeId + ", msg=" + reqMsg0 + ']', e);

                            try {
                                cctx.gridIO().sendToCustomTopic(nodeId,
                                    DFLT_INITIAL_SNAPSHOT_TOPIC,
                                    new SnapshotResponseMessage(reqMsg0.snapshotName(), e.getMessage()),
                                    SYSTEM_POOL);
                            }
                            catch (IgniteCheckedException ex0) {
                                U.error(log, "Fail to send the response message with processing snapshot request " +
                                    "error [request=" + reqMsg0 + ", nodeId=" + nodeId + ']', ex0);
                            }
                        }
                    }
                    else if (msg instanceof SnapshotResponseMessage) {
                        SnapshotResponseMessage respMsg0 = (SnapshotResponseMessage)msg;

                        RemoteSnapshotFuture fut0 = rmtSnpReq.get();

                        if (fut0 == null || !fut0.snpName.equals(respMsg0.snapshotName())) {
                            if (log.isInfoEnabled()) {
                                log.info("A stale snapshot response message has been received. Will be ignored " +
                                    "[fromNodeId=" + nodeId + ", response=" + respMsg0 + ']');
                            }

                            return;
                        }

                        if (respMsg0.errorMessage() != null) {
                            fut0.onDone(new IgniteCheckedException("Request cancelled. The snapshot operation stopped " +
                                "on the remote node with an error: " + respMsg0.errorMessage()));
                        }
                    }
                }
                finally {
                    busyLock.leaveBusy();
                }
            }
        });

        cctx.gridEvents().addDiscoveryEventListener(discoLsnr = (evt, discoCache) -> {
            if (!busyLock.enterBusy())
                return;

            try {
                for (SnapshotTask sctx : locSnpTasks.values()) {
                    if (sctx.sourceNodeId().equals(evt.eventNode().id())) {
                        sctx.acceptException(new ClusterTopologyCheckedException("The node which requested snapshot " +
                            "creation has left the grid"));

                        sctx.closeAsync();
                    }
                }

                RemoteSnapshotFuture snpTrFut = rmtSnpReq.get();

                if (snpTrFut != null && snpTrFut.rmtNodeId.equals(evt.eventNode().id())) {
                    snpTrFut.onDone(new ClusterTopologyCheckedException("The node from which a snapshot has been " +
                        "requested left the grid"));
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }, EVT_NODE_LEFT, EVT_NODE_FAILED);

        // Remote snapshot handler.
        cctx.kernalContext().io().addTransmissionHandler(DFLT_INITIAL_SNAPSHOT_TOPIC, new TransmissionHandler() {
            /** {@inheritDoc} */
            @Override public void onException(UUID nodeId, Throwable err) {
                RemoteSnapshotFuture fut = rmtSnpReq.get();

                if (fut == null)
                    return;

                if (fut.rmtNodeId.equals(nodeId)) {
                    fut.onDone(err);

                    if(snpLsnr != null)
                        snpLsnr.onException(nodeId, err);
                }
            }

            /** {@inheritDoc} */
            @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
                Integer partId = (Integer)fileMeta.params().get(SNP_PART_ID_PARAM);
                String snpName = (String)fileMeta.params().get(SNP_NAME_PARAM);
                String rmtDbNodePath = (String)fileMeta.params().get(SNP_DB_NODE_PATH_PARAM);
                String cacheDirName = (String)fileMeta.params().get(SNP_CACHE_DIR_NAME_PARAM);

                RemoteSnapshotFuture transFut = rmtSnpReq.get();

                if (transFut == null || !transFut.snpName.equals(snpName)) {
                    throw new TransmissionCancelledException("Stale snapshot transmission will be ignored " +
                        "[snpName=" + snpName + ", transFut=" + transFut + ']');
                }

                assert transFut.snpName.equals(snpName) &&  transFut.rmtNodeId.equals(nodeId) :
                    "Another transmission in progress [fut=" + transFut + ", nodeId=" + snpName + ", nodeId=" + nodeId +']';

                try {
                    File cacheDir = U.resolveWorkDirectory(tmpWorkDir.getAbsolutePath(),
                        cacheSnapshotPath(snpName, rmtDbNodePath, cacheDirName),
                        false);

                    return new File(cacheDir, getPartitionFileName(partId)).getAbsolutePath();
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }

            /**
             * @param snpTrans Current snapshot transmission.
             * @param rmtNodeId Remote node which sends partition.
             * @param grpPartId Pair of group id and its partition id.
             */
            private void finishRecover(
                RemoteSnapshotFuture snpTrans,
                UUID rmtNodeId,
                GroupPartitionId grpPartId
            ) {
                FilePageStore pageStore = null;

                try {
                    pageStore = snpTrans.stores.remove(grpPartId);

                    pageStore.finishRecover();

                    String partAbsPath = pageStore.getFileAbsolutePath();

                    cctx.kernalContext().closure().runLocalSafe(() -> {
                        if (snpLsnr == null)
                            return;

                        snpLsnr.onPartition(rmtNodeId,
                            new File(partAbsPath),
                            grpPartId.getGroupId(),
                            grpPartId.getPartitionId());
                    });

                    if (snpTrans.partsLeft.decrementAndGet() == 0) {
                        cctx.kernalContext().closure().runLocalSafe(() -> {
                            if (snpLsnr == null)
                                return;

                            snpLsnr.onEnd(rmtNodeId);
                        });

                        snpTrans.onDone(true);
                    }
                }
                catch (StorageException e) {
                    throw new IgniteException(e);
                }
                finally {
                    U.closeQuiet(pageStore);
                }
            }

            /** {@inheritDoc} */
            @Override public Consumer<ByteBuffer> chunkHandler(UUID nodeId, TransmissionMeta initMeta) {
                Integer grpId = (Integer)initMeta.params().get(SNP_GRP_ID_PARAM);
                Integer partId = (Integer)initMeta.params().get(SNP_PART_ID_PARAM);
                String snpName = (String)initMeta.params().get(SNP_NAME_PARAM);

                GroupPartitionId grpPartId = new GroupPartitionId(grpId, partId);
                RemoteSnapshotFuture snpTrFut = rmtSnpReq.get();

                if (snpTrFut == null || !snpTrFut.snpName.equals(snpName)) {
                    throw new TransmissionCancelledException("Stale snapshot transmission will be ignored " +
                        "[snpName=" + snpName + ", grpId=" + grpId + ", partId=" + partId + ", snpTrFut=" + snpTrFut + ']');
                }

                assert snpTrFut.snpName.equals(snpName) &&  snpTrFut.rmtNodeId.equals(nodeId) :
                    "Another transmission in progress [snpTrFut=" + snpTrFut + ", nodeId=" + snpName + ", nodeId=" + nodeId +']';

                FilePageStore pageStore = snpTrFut.stores.get(grpPartId);

                if (pageStore == null) {
                    throw new IgniteException("Partition must be loaded before applying snapshot delta pages " +
                        "[snpName=" + snpName + ", grpId=" + grpId + ", partId=" + partId + ']');
                }

                pageStore.beginRecover();

                // No snapshot delta pages received. Finalize recovery.
                if (initMeta.count() == 0) {
                    finishRecover(snpTrFut,
                        nodeId,
                        grpPartId);
                }

                return new Consumer<ByteBuffer>() {
                    final LongAdder transferred = new LongAdder();

                    @Override public void accept(ByteBuffer buff) {
                        try {
                            assert initMeta.count() != 0 : initMeta;

                            RemoteSnapshotFuture fut0 = rmtSnpReq.get();

                            if (fut0 == null || !fut0.equals(snpTrFut) || fut0.isCancelled()) {
                                throw new TransmissionCancelledException("Snapshot request is cancelled " +
                                    "[snpName=" + snpName + ", grpId=" + grpId + ", partId=" + partId + ']');
                            }

                            pageStore.write(PageIO.getPageId(buff), buff, 0, false);

                            transferred.add(buff.capacity());

                            if (transferred.longValue() == initMeta.count()) {
                                finishRecover(snpTrFut,
                                    nodeId,
                                    grpPartId);
                            }
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException(e);
                        }
                    }
                };
            }

            /** {@inheritDoc} */
            @Override public Consumer<File> fileHandler(UUID nodeId, TransmissionMeta initMeta) {
                Integer grpId = (Integer)initMeta.params().get(SNP_GRP_ID_PARAM);
                Integer partId = (Integer)initMeta.params().get(SNP_PART_ID_PARAM);
                String snpName = (String)initMeta.params().get(SNP_NAME_PARAM);

                assert grpId != null;
                assert partId != null;
                assert snpName != null;
                assert storeFactory != null;

                RemoteSnapshotFuture transFut = rmtSnpReq.get();

                if (transFut == null) {
                    throw new IgniteException("Snapshot transmission with given name doesn't exists " +
                        "[snpName=" + snpName + ", grpId=" + grpId + ", partId=" + partId + ']');
                }

                return new Consumer<File>() {
                    @Override public void accept(File file) {
                        RemoteSnapshotFuture fut0 = rmtSnpReq.get();

                        if (fut0 == null || !fut0.equals(transFut) || fut0.isCancelled()) {
                            throw new TransmissionCancelledException("Snapshot request is cancelled [snpName=" + snpName +
                                ", grpId=" + grpId + ", partId=" + partId + ']');
                        }

                        busyLock.enterBusy();

                        try {
                            FilePageStore pageStore = (FilePageStore)storeFactory
                                .apply(grpId, false)
                                .createPageStore(getFlagByPartId(partId),
                                    file::toPath,
                                    new LongAdderMetric("NO_OP", null));

                            transFut.stores.put(new GroupPartitionId(grpId, partId), pageStore);

                            pageStore.init();
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException(e);
                        }
                        finally {
                            busyLock.leaveBusy();
                        }
                    }
                };
            }
        });
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        busyLock.block();

        try {
            for (SnapshotTask sctx : locSnpTasks.values()) {
                // Try stop all snapshot processing if not yet.
                sctx.close(new NodeStoppingException("Snapshot has been cancelled due to the local node " +
                    "is stopping"));
            }

            locSnpTasks.clear();

            RemoteSnapshotFuture snpTrFut = rmtSnpReq.get();

            if (snpTrFut != null)
                snpTrFut.cancel();

            snpRunner.shutdown();

            cctx.kernalContext().io().removeMessageListener(DFLT_INITIAL_SNAPSHOT_TOPIC);
            cctx.kernalContext().event().removeDiscoveryEventListener(discoLsnr);
            cctx.kernalContext().io().removeTransmissionHandler(DFLT_INITIAL_SNAPSHOT_TOPIC);
        }
        finally {
            busyLock.unblock();
        }
    }

    /**
     * @return Relative configured path of presistence data storage directory for the local node.
     * Example: {@code snapshotWorkDir/db/IgniteNodeName0}
     */
    public static String relativeNodePath(PdsFolderSettings pcfg) {
        return Paths.get(DB_DEFAULT_FOLDER, pcfg.folderName()).toString();
    }

    /**
     * @param snpLsnr Snapshot listener instance.
     */
    public void addSnapshotListener(SnapshotListener snpLsnr) {
        this.snpLsnr = snpLsnr;
    }

    /**
     * @param snpName Snapshot name.
     * @return Local snapshot directory for snapshot with given name.
     */
    public File snapshotLocalDir(String snpName) {
        assert locSnpDir != null;

        return new File(locSnpDir, snpName);
    }

    /**
     * @return Node snapshot working directory.
     */
    public File snapshotTempDir() {
        assert tmpWorkDir != null;

        return tmpWorkDir;
    }

    /**
     * @param snpName Unique snapshot name.
     * @return Future which will be completed when snapshot is done.
     */
    IgniteInternalFuture<Boolean> createLocalSnapshot(String snpName, List<Integer> grpIds) {
        // Collection of pairs group and appropratate cache partition to be snapshotted.
        Map<Integer, GridIntList> parts = grpIds.stream()
            .collect(Collectors.toMap(grpId -> grpId,
                grpId -> {
                    GridIntList grps = new GridIntList();

                    cctx.cache()
                        .cacheGroup(grpId)
                        .topology()
                        .currentLocalPartitions()
                        .forEach(p -> grps.add(p.id()));

                    grps.add(INDEX_PARTITION);

                    return grps;
                }));

        try {
            return runLocalSnapshotTask(snpName,
                cctx.localNodeId(),
                parts,
                snpRunner,
                localSnapshotSender(snpName));
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * @param parts Collection of pairs group and appropratate cache partition to be snapshotted.
     * @param rmtNodeId The remote node to connect to.
     * @return Snapshot name.
     */
    public IgniteInternalFuture<Boolean> createRemoteSnapshot(UUID rmtNodeId, Map<Integer, Set<Integer>> parts) {
        ClusterNode rmtNode = cctx.discovery().node(rmtNodeId);

        if (!nodeSupports(rmtNode, PERSISTENCE_CACHE_SNAPSHOT))
            return new GridFinishedFuture<>(new IgniteCheckedException("Snapshot on remote node is not supported: " + rmtNode.id()));

        if (rmtNode == null) {
            return new GridFinishedFuture<>(new ClusterTopologyCheckedException("Snapshot request cannot be performed. " +
                "Remote node left the grid [rmtNodeId=" + rmtNodeId + ']'));
        }

        for (Map.Entry<Integer, Set<Integer>> e : parts.entrySet()) {
            int grpId = e.getKey();

            GridDhtPartitionMap partMap = cctx.cache()
                .cacheGroup(grpId)
                .topology()
                .partitions(rmtNodeId);

            Set<Integer> owningParts = partMap.entrySet()
                .stream()
                .filter(p -> p.getValue() == GridDhtPartitionState.OWNING)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

            if (!owningParts.containsAll(e.getValue())) {
                Set<Integer> substract = new HashSet<>(e.getValue());

                substract.removeAll(owningParts);

                return new GridFinishedFuture<>(new IgniteCheckedException("Only owning partitions allowed to be " +
                    "requested from the remote node [rmtNodeId=" + rmtNodeId + ", grpId=" + grpId +
                    ", missed=" + substract + ']'));
            }
        }

        String snpName = "snapshot_" + UUID.randomUUID().getMostSignificantBits();

        RemoteSnapshotFuture snpTransFut = new RemoteSnapshotFuture(rmtNodeId, snpName,
            parts.values().stream().mapToInt(Set::size).sum());

        busyLock.enterBusy();
        SnapshotRequestMessage msg0;

        try {
            msg0 = new SnapshotRequestMessage(snpName,
                parts.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey,
                        e -> GridIntList.valueOf(e.getValue()))));

            RemoteSnapshotFuture fut = rmtSnpReq.get();

            try {
                if (fut != null)
                    fut.get(DFLT_SNAPSHOT_TIMEOUT, TimeUnit.MILLISECONDS);
            }
            catch (IgniteCheckedException e) {
                if (log.isInfoEnabled())
                    log.info("The previous snapshot request finished with an excpetion:" + e.getMessage());
            }

            try {
                if (rmtSnpReq.compareAndSet(null, snpTransFut)) {
                    cctx.gridIO().sendOrderedMessage(rmtNode, DFLT_INITIAL_SNAPSHOT_TOPIC, msg0, SYSTEM_POOL,
                        Long.MAX_VALUE, true);
                }
                else
                    return new GridFinishedFuture<>(new IgniteCheckedException("Snapshot request has been concurrently interrupted."));

            }
            catch (IgniteCheckedException e) {
                rmtSnpReq.compareAndSet(snpTransFut, null);

                return new GridFinishedFuture<>(e);
            }
        }
        finally {
            busyLock.leaveBusy();
        }

        if (log.isInfoEnabled()) {
            log.info("Snapshot request is sent to the remote node [rmtNodeId=" + rmtNodeId +
                ", msg0=" + msg0 + ", snpTransFut=" + snpTransFut +
                ", topVer=" + cctx.discovery().topologyVersionEx() + ']');
        }

        return snpTransFut;
    }

    /**
     * @param grps List of cache groups which will be destroyed.
     */
    public void onCacheGroupsStopped(List<Integer> grps) {
        for (SnapshotTask sctx : locSnpTasks.values()) {
            Set<Integer> snpGrps = sctx.partitions().stream()
                .map(GroupPartitionId::getGroupId)
                .collect(Collectors.toSet());

            Set<Integer> retain = new HashSet<>(grps);
            retain.retainAll(snpGrps);

            if (!retain.isEmpty()) {
                sctx.acceptException(new IgniteCheckedException("Snapshot has been interrupted due to some of the required " +
                    "cache groups stopped: " + retain));
            }
        }
    }

    /**
     * @param snpName Unique snapshot name.
     * @param srcNodeId Node id which cause snapshot operation.
     * @param parts Collection of pairs group and appropratate cache partition to be snapshotted.
     * @param snpSndr Factory which produces snapshot receiver instance.
     * @return Future which will be completed when snapshot is done.
     */
    IgniteInternalFuture<Boolean> runLocalSnapshotTask(
        String snpName,
        UUID srcNodeId,
        Map<Integer, GridIntList> parts,
        Executor exec,
        SnapshotFileSender snpSndr
    ) {
        if (!busyLock.enterBusy())
            return new GridFinishedFuture<>(new IgniteCheckedException("Snapshot manager is stopping [locNodeId=" + cctx.localNodeId() + ']'));

        try {
            SnapshotTask snpTask = putSnapshotTask(snpName,
                cctx.localNodeId(),
                parts,
                snpRunner,
                snpSndr);

            snpTask.run();

            // Snapshot is still in the INIT state. beforeCheckpoint has been skipped
            // due to checkpoint aready running and we need to schedule the next one
            // right afther current will be completed.
            dbMgr.forceCheckpoint(String.format(SNAPSHOT_CP_REASON, snpName));

            snpTask.awaitStarted();

            return snpTask.snapshotFuture();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param snpName Unique snapshot name.
     * @param srcNodeId Node id which cause snapshot operation.
     * @param parts Collection of pairs group and appropratate cache partition to be snapshotted.
     * @param snpSndr Factory which produces snapshot receiver instance.
     * @return Snapshot operation task which should be registered on checkpoint to run.
     * @throws IgniteCheckedException If fails.
     */
    SnapshotTask putSnapshotTask(
        String snpName,
        UUID srcNodeId,
        Map<Integer, GridIntList> parts,
        Executor exec,
        SnapshotFileSender snpSndr
    ) throws IgniteCheckedException {
        if (locSnpTasks.containsKey(snpName))
            throw new IgniteCheckedException("Snapshot with requested name is already scheduled: " + snpName);

        SnapshotTask snpTask = locSnpTasks.computeIfAbsent(snpName,
            snpName0 -> new SnapshotTask(cctx,
                srcNodeId,
                snpName0,
                tmpWorkDir,
                exec,
                ioFactory,
                snpSndr,
                parts));

        snpTask.snapshotFuture()
            .listen(f -> locSnpTasks.remove(snpName));

        return snpTask;
    }

    /**
     * @param snpName Snapshot name to associate sender with.
     * @return Snapshot receiver instance.
     */
    SnapshotFileSender localSnapshotSender(String snpName) throws IgniteCheckedException {
        File snpLocDir = snapshotLocalDir(snpName);

        return new LocalSnapshotFileSender(log,
            () -> {
                // Relative path to snapshot storage of local node.
                // Example: snapshotWorkDir/db/IgniteNodeName0
                String dbNodePath = relativeNodePath(cctx.kernalContext().pdsFolderResolver().resolveFolders());

                return U.resolveWorkDirectory(snpLocDir.getAbsolutePath(), dbNodePath, false);
            },
            ioFactory,
            storeFactory,
            cctx.kernalContext()
                .cacheObjects()
                .binaryWriter(snpLocDir.getAbsolutePath()),
            cctx.kernalContext()
                .marshallerContext()
                .marshallerMappingWriter(cctx.kernalContext(), snpLocDir.getAbsolutePath()),
            pageSize);
    }

    /**
     * @param snpName Snapshot name.
     * @param rmtNodeId Remote node id to send snapshot to.
     * @return Snapshot sender instance.
     */
    SnapshotFileSender remoteSnapshotSender(String snpName, UUID rmtNodeId) {
        return new RemoteSnapshotFileSender(log,
            () -> relativeNodePath(cctx.kernalContext().pdsFolderResolver().resolveFolders()),
            cctx.gridIO().openTransmissionSender(rmtNodeId, DFLT_INITIAL_SNAPSHOT_TOPIC),
            errMsg -> cctx.gridIO().sendToCustomTopic(rmtNodeId,
                DFLT_INITIAL_SNAPSHOT_TOPIC,
                new SnapshotResponseMessage(snpName, errMsg),
                SYSTEM_POOL),
            snpName);
    }

    /**
     * @return The executor service used to run snapshot tasks.
     */
    ExecutorService snapshotExecutorService() {
        assert snpRunner != null;

        return snpRunner;
    }

    /**
     * @param ioFactory Factory to create IO interface over a page stores.
     */
    void ioFactory(FileIOFactory ioFactory) {
        this.ioFactory = ioFactory;
    }

    /**
     * @param nodeId Remote node id on which requests has been registered.
     * @return Snapshot future related to given node id.
     */
    IgniteInternalFuture<Boolean> snapshotRemoteRequest(UUID nodeId) {
        return locSnpTasks.values().stream()
            .filter(t -> t.type() == RemoteSnapshotFileSender.class && t.sourceNodeId().equals(nodeId))
            .map(SnapshotTask::snapshotFuture)
            .findFirst()
            .orElse(null);
    }

    /**
     * @param rslvr RDS resolver.
     * @param dirPath Relative working directory path.
     * @param errorMsg Error message in case of make direcotry fail.
     * @return Resolved working direcory.
     * @throws IgniteCheckedException If fails.
     */
    private static File initWorkDirectory(
        PdsFolderSettings rslvr,
        String dirPath,
        IgniteLogger log,
        String errorMsg
    ) throws IgniteCheckedException {
        File rmtSnpDir = U.resolveWorkDirectory(rslvr.persistentStoreRootPath().getAbsolutePath(), dirPath, false);

        File target = new File(rmtSnpDir, rslvr.folderName());

        U.ensureDirectory(target, errorMsg, log);

        return target;
    }

    /**
     * @param dbNodePath Persistence node path.
     * @param snpName Snapshot name.
     * @param cacheDirName Cache directory name.
     * @return Relative cache path.
     */
    private static String cacheSnapshotPath(String snpName, String dbNodePath, String cacheDirName) {
        return Paths.get(snpName, dbNodePath, cacheDirName).toString();
    }

    /**
     *
     */
    private class RemoteSnapshotFuture extends GridFutureAdapter<Boolean> {
        /** Remote node id to request snapshot from. */
        private final UUID rmtNodeId;

        /** Snapshot name to create on remote. */
        private final String snpName;

        /** Collection of partition to be received. */
        private final Map<GroupPartitionId, FilePageStore> stores = new ConcurrentHashMap<>();

        /** Counter which show how many partitions left to be received. */
        private final AtomicInteger partsLeft;

        /**
         * @param cnt Partitions to receive.
         */
        public RemoteSnapshotFuture(UUID rmtNodeId, String snpName, int cnt) {
            this.rmtNodeId = rmtNodeId;
            this.snpName = snpName;
            partsLeft = new AtomicInteger(cnt);
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            if (onCancelled()) {
                // Close non finished file storages
                for (Map.Entry<GroupPartitionId, FilePageStore> entry : stores.entrySet()) {
                    FilePageStore store = entry.getValue();

                    try {
                        store.stop(true);
                    }
                    catch (StorageException e) {
                        log.warning("Error stopping received file page store", e);
                    }
                }
            }

            return isCancelled();
        }

        /** {@inheritDoc} */
        @Override protected boolean onDone(@Nullable Boolean res, @Nullable Throwable err, boolean cancel) {
            assert err != null || cancel || stores.isEmpty() : "Not all file storages processed: " + stores;

            rmtSnpReq.compareAndSet(this, null);

            return super.onDone(res, err, cancel);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            RemoteSnapshotFuture future = (RemoteSnapshotFuture)o;

            return rmtNodeId.equals(future.rmtNodeId) &&
                snpName.equals(future.snpName);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(rmtNodeId, snpName);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RemoteSnapshotFuture.class, this);
        }
    }

    /**
     *
     */
    private static class SerialExecutor implements Executor {
        /** */
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        /** */
        private final Executor executor;

        /** */
        private volatile Runnable active;

        /**
         * @param executor Executor to run tasks on.
         */
        public SerialExecutor(Executor executor) {
            this.executor = executor;
        }

        /** {@inheritDoc} */
        @Override public synchronized void execute(final Runnable r) {
            tasks.offer(new Runnable() {
                /** {@inheritDoc} */
                @Override public void run() {
                    try {
                        r.run();
                    }
                    finally {
                        scheduleNext();
                    }
                }
            });

            if (active == null) {
                scheduleNext();
            }
        }

        /**
         *
         */
        protected synchronized void scheduleNext() {
            if ((active = tasks.poll()) != null) {
                executor.execute(active);
            }
        }
    }

    /**
     *
     */
    private static class RemoteSnapshotFileSender extends SnapshotFileSender {
        /** The sender which sends files to remote node. */
        private final GridIoManager.TransmissionSender sndr;

        /** Error handler which will be triggered in case of transmission sedner not started yet. */
        private final IgniteThrowableConsumer<String> errHnd;

        /** Relative node path initializer. */
        private final IgniteThrowableSupplier<String> initPath;

        /** Snapshot name */
        private final String snpName;

        /** Local node persistent directory with consistent id. */
        private String relativeNodePath;

        /**
         * @param log Ignite logger.
         * @param sndr File sender instance.
         * @param errHnd Snapshot error handler if transmission sender not started yet.
         * @param snpName Snapshot name.
         */
        public RemoteSnapshotFileSender(
            IgniteLogger log,
            IgniteThrowableSupplier<String> initPath,
            GridIoManager.TransmissionSender sndr,
            IgniteThrowableConsumer<String> errHnd,
            String snpName
        ) {
            super(log);

            this.sndr = sndr;
            this.errHnd = errHnd;
            this.snpName = snpName;
            this.initPath = initPath;
        }

        /** {@inheritDoc} */
        @Override protected void init() throws IgniteCheckedException {
            relativeNodePath = initPath.get();

            if (relativeNodePath == null)
                throw new IgniteException("Relative node path cannot be empty.");
        }

        /** {@inheritDoc} */
        @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long len) {
            try {
                assert part.exists();
                assert len > 0 : "Requested partitions has incorrect file length " +
                    "[pair=" + pair + ", cacheDirName=" + cacheDirName + ']';

                sndr.send(part, 0, len, transmissionParams(snpName, cacheDirName, pair), TransmissionPolicy.FILE);

                if (log.isInfoEnabled()) {
                    log.info("Partition file has been send [part=" + part.getName() + ", pair=" + pair +
                        ", length=" + len + ']');
                }
            }
            catch (TransmissionCancelledException e) {
                if (log.isInfoEnabled()) {
                    log.info("Transmission partition file has been interrupted [part=" + part.getName() +
                        ", pair=" + pair + ']');
                }
            }
            catch (IgniteCheckedException | InterruptedException | IOException e) {
                U.error(log, "Error sending partition file [part=" + part.getName() + ", pair=" + pair +
                    ", length=" + len + ']', e);

                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void sendDelta0(File delta, String cacheDirName, GroupPartitionId pair) {
            try {
                sndr.send(delta, transmissionParams(snpName, cacheDirName, pair), TransmissionPolicy.CHUNK);

                if (log.isInfoEnabled())
                    log.info("Delta pages storage has been send [part=" + delta.getName() + ", pair=" + pair + ']');
            }
            catch (TransmissionCancelledException e) {
                if (log.isInfoEnabled()) {
                    log.info("Transmission delta pages has been interrupted [part=" + delta.getName() +
                        ", pair=" + pair + ']');
                }
            }
            catch (IgniteCheckedException | InterruptedException | IOException e) {
                U.error(log, "Error sending delta file  [part=" + delta.getName() + ", pair=" + pair + ']', e);

                throw new IgniteException(e);
            }
        }

        /**
         * @param cacheDirName Cache directory name.
         * @param pair Cache group id with corresponding partition id.
         * @return Map of params.
         */
        private Map<String, Serializable> transmissionParams(String snpName, String cacheDirName, GroupPartitionId pair) {
            Map<String, Serializable> params = new HashMap<>();

            params.put(SNP_GRP_ID_PARAM, pair.getGroupId());
            params.put(SNP_PART_ID_PARAM, pair.getPartitionId());
            params.put(SNP_DB_NODE_PATH_PARAM, relativeNodePath);
            params.put(SNP_CACHE_DIR_NAME_PARAM, cacheDirName);
            params.put(SNP_NAME_PARAM, snpName);

            return params;
        }

        /** {@inheritDoc} */
        @Override public void close0(@Nullable Throwable th) {
            try {
                if (th != null && !sndr.opened())
                    errHnd.accept(th.getMessage());
            }
            catch (IgniteCheckedException e) {
                th.addSuppressed(e);
            }

            U.closeQuiet(sndr);

            if (th == null) {
                if (log.isInfoEnabled())
                    log.info("The remote snapshot sender closed normally [snpName=" + snpName + ']');
            }
            else {
                U.warn(log, "The remote snapshot sender closed due to an error occurred while processing " +
                    "snapshot operation [snpName=" + snpName + ']', th);
            }
        }
    }

    /**
     *
     */
    private static class LocalSnapshotFileSender extends SnapshotFileSender {
        /**
         * Local node snapshot directory calculated on snapshot directory.
         */
        private File dbNodeSnpDir;

        /** Facotry to produce IO interface over a file. */
        private final FileIOFactory ioFactory;

        /** Factory to create page store for restore. */
        private final BiFunction<Integer, Boolean, FilePageStoreFactory> storeFactory;

        /** Store binary files. */
        private final BinaryTypeWriter binaryWriter;

        /** Marshaller mapping writer. */
        private final MarshallerMappingWriter mappingWriter;

        /** Size of page. */
        private final int pageSize;

        /** Additional snapshot meta information which will be written on disk. */
        private final IgniteThrowableSupplier<File> initPath;

        /**
         * @param log Ignite logger to use.
         * @param ioFactory Facotry to produce IO interface over a file.
         * @param storeFactory Factory to create page store for restore.
         * @param pageSize Size of page.
         */
        public LocalSnapshotFileSender(
            IgniteLogger log,
            IgniteThrowableSupplier<File> initPath,
            FileIOFactory ioFactory,
            BiFunction<Integer, Boolean, FilePageStoreFactory> storeFactory,
            BinaryTypeWriter binaryWriter,
            MarshallerMappingWriter mappingWriter,
            int pageSize
        ) {
            super(log);

            this.ioFactory = ioFactory;
            this.storeFactory = storeFactory;
            this.pageSize = pageSize;
            this.binaryWriter = binaryWriter;
            this.mappingWriter = mappingWriter;
            this.initPath = initPath;
        }

        /** {@inheritDoc} */
        @Override protected void init() throws IgniteCheckedException {
            dbNodeSnpDir = initPath.get();

            if (dbNodeSnpDir == null)
                throw new IgniteException("Local snapshot directory cannot be null");
        }

        /** {@inheritDoc} */
        @Override public void sendCacheConfig0(File ccfg, String cacheDirName) {
            assert dbNodeSnpDir != null;

            try {
                File cacheDir = U.resolveWorkDirectory(dbNodeSnpDir.getAbsolutePath(), cacheDirName, false);

                copy(ccfg, new File(cacheDir, ccfg.getName()), ccfg.length());
            }
            catch (IgniteCheckedException | IOException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void sendMarshallerMeta0(List<Map<Integer, MappedName>> mappings) {
            if (mappings == null)
                return;

            for (int platformId = 0; platformId < mappings.size(); platformId++) {
                Map<Integer, MappedName> cached = mappings.get(platformId);

                try {
                    addPlatformMappings((byte)platformId,
                        cached,
                        (typeId, clsName) -> true,
                        (typeId, mapping) -> {
                        },
                        mappingWriter);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void sendBinaryMeta0(Map<Integer, BinaryType> types) {
            if (types == null)
                return;

            for (Map.Entry<Integer, BinaryType> e : types.entrySet())
                binaryWriter.writeMeta(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long len) {
            try {
                if (len == 0)
                    return;

                File cacheDir = U.resolveWorkDirectory(dbNodeSnpDir.getAbsolutePath(), cacheDirName, false);

                File snpPart = new File(cacheDir, part.getName());

                if (!snpPart.exists() || snpPart.delete())
                    snpPart.createNewFile();

                copy(part, snpPart, len);

                if (log.isInfoEnabled()) {
                    log.info("Partition has been snapshotted [snapshotDir=" + dbNodeSnpDir.getAbsolutePath() +
                        ", cacheDirName=" + cacheDirName + ", part=" + part.getName() +
                        ", length=" + part.length() + ", snapshot=" + snpPart.getName() + ']');
                }
            }
            catch (IOException | IgniteCheckedException ex) {
                throw new IgniteException(ex);
            }
        }

        /** {@inheritDoc} */
        @Override public void sendDelta0(File delta, String cacheDirName, GroupPartitionId pair) {
            File snpPart = getPartitionFile(dbNodeSnpDir, cacheDirName, pair.getPartitionId());

            if (log.isInfoEnabled()) {
                log.info("Start partition snapshot recovery with the given delta page file [part=" + snpPart +
                    ", delta=" + delta + ']');
            }

            try (FileIO fileIo = ioFactory.create(delta, READ);
                 FilePageStore pageStore = (FilePageStore)storeFactory
                     .apply(pair.getGroupId(), false)
                     .createPageStore(getFlagByPartId(pair.getPartitionId()),
                         snpPart::toPath,
                         new LongAdderMetric("NO_OP", null))
            ) {
                ByteBuffer pageBuf = ByteBuffer.allocate(pageSize)
                    .order(ByteOrder.nativeOrder());

                long totalBytes = fileIo.size();

                assert totalBytes % pageSize == 0 : "Given file with delta pages has incorrect size: " + fileIo.size();

                pageStore.beginRecover();

                for (long pos = 0; pos < totalBytes; pos += pageSize) {
                    long read = fileIo.readFully(pageBuf, pos);

                    assert read == pageBuf.capacity();

                    pageBuf.flip();

                    long pageId = PageIO.getPageId(pageBuf);

                    int crc32 = FastCrc.calcCrc(pageBuf, pageBuf.limit());

                    int crc = PageIO.getCrc(pageBuf);

                    if (log.isDebugEnabled()) {
                        log.debug("Read page given delta file [path=" + delta.getName() +
                            ", pageId=" + pageId + ", pos=" + pos + ", pages=" + (totalBytes / pageSize) +
                            ", crcBuff=" + crc32 + ", crcPage=" + crc + ']');
                    }

                    pageBuf.rewind();

                    pageStore.write(PageIO.getPageId(pageBuf), pageBuf, 0, false);

                    pageBuf.flip();
                }

                pageStore.finishRecover();
            }
            catch (IOException | IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override protected void close0(@Nullable Throwable th) {
            if (th == null) {
                if (log.isInfoEnabled())
                    log.info("Local snapshot sender closed, resouces released [dbNodeSnpDir=" + dbNodeSnpDir + ']');
            }
            else {
                dbNodeSnpDir.delete();

                U.error(log, "Local snapshot sender closed due to an error occurred", th);
            }
        }

        /**
         * @param from Copy from file.
         * @param to Copy data to file.
         * @param length Number of bytes to copy from beginning.
         * @throws IOException If fails.
         */
        private void copy(File from, File to, long length) throws IOException {
            try (FileIO src = ioFactory.create(from, READ);
                 FileChannel dest = new FileOutputStream(to).getChannel()) {
                if (src.size() < length)
                    throw new IgniteException("The source file to copy has to enought length [expected=" + length + ", actual=" + src.size() + ']');

                src.position(0);

                long written = 0;

                while (written < length)
                    written += src.transferTo(written, length - written, dest);
            }
        }
    }
}
