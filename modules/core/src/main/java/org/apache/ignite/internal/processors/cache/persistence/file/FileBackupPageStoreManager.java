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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointFuture;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.backup.BackupProcessTask;
import org.apache.ignite.internal.processors.cache.persistence.backup.FileTemporaryStore;
import org.apache.ignite.internal.processors.cache.persistence.backup.IgniteBackupPageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.backup.TemporaryStore;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PagesAllocationRange;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperationAdapter;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.util.Optional.ofNullable;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;

/** */
public class FileBackupPageStoreManager extends GridCacheSharedManagerAdapter
    implements IgniteBackupPageStoreManager {
    /** */
    public static final String PART_DELTA_TEMPLATE = PART_FILE_TEMPLATE + ".delta";

    /** */
    private static final String BACKUP_CP_REASON = "Wakeup for checkpoint to take backup [id=%s, grpId=%s, parts=%s]";

    /** Base working directory for saving copied pages. */
    private final File backupWorkDir;

    /** Factory to working with {@link TemporaryStore} as file storage. */
    private final FileIOFactory ioFactory;

    /** Tracking partition files over all running snapshot processes. */
    private final ConcurrentMap<GroupPartitionId, AtomicInteger> trackMap = new ConcurrentHashMap<>();

    /** Keep only the first page error. */
    private final ConcurrentMap<GroupPartitionId, IgniteCheckedException> pageTrackErrors = new ConcurrentHashMap<>();

    /** Collection of backup stores indexed by [grpId, partId] key. */
    private final Map<GroupPartitionId, TemporaryStore> backupStores = new ConcurrentHashMap<>();

    /** */
    private final IgniteLogger log;

    /** */
    private final int pageSize;

    /** */
    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();

    /** Thread local with buffers for handling copy-on-write over {@link PageStore} events. */
    private ThreadLocal<ByteBuffer> threadPageBuff;

    /** A byte array to store intermediate calculation results of process handling page writes. */
    private ThreadLocal<byte[]> threadTempArr;

    /** */
    public FileBackupPageStoreManager(GridKernalContext ctx) throws IgniteCheckedException {
        assert CU.isPersistenceEnabled(ctx.config());

        log = ctx.log(getClass());
        pageSize = ctx.config().getDataStorageConfiguration().getPageSize();

        backupWorkDir = U.resolveWorkDirectory(ctx.config().getWorkDirectory(),
            DataStorageConfiguration.DFLT_BACKUP_DIRECTORY,
            true);

        U.ensureDirectory(backupWorkDir, "backup store working directory", log);

        ioFactory = new RandomAccessFileIOFactory();
    }

    /**
     * @param tmpDir Temporary directory to store files.
     * @param partId Cache partition identifier.
     * @return A file representation.
     */
    public static File getPartionDeltaFile(File tmpDir, int partId) {
        return new File(tmpDir, String.format(PART_DELTA_TEMPLATE, partId));
    }

    /**
     * @param ccfg Cache configuration.
     * @param partId Partiton identifier.
     * @return The cache partiton file.
     */
    private File resolvePartitionFileCfg(CacheConfiguration ccfg, int partId) {
        File cacheDir = ((FilePageStoreManager)cctx.pageStore()).cacheWorkDir(ccfg);

        return getPartitionFile(cacheDir, partId);
    }

    /**
     * @param ccfg Cache configuration.
     * @param partId Partiton identifier.
     * @return The cache partiton delta file.
     */
    private File resolvePartitionDeltaFileCfg(CacheConfiguration ccfg, int partId) {
        File cacheTempDir = cacheWorkDir(backupWorkDir, ccfg);

        return getPartionDeltaFile(cacheTempDir, partId);
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        setThreadPageBuff(ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder())));

        threadTempArr = ThreadLocal.withInitial(() -> new byte[pageSize]);
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        // Nothing to do. Backups are created on demand.
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        for (TemporaryStore store : backupStores.values())
            U.closeQuiet(store);

        backupStores.clear();
        trackMap.clear();
        pageTrackErrors.clear();
    }

    /** {@inheritDoc} */
    @Override public void backup(
        int idx,
        int grpId,
        Set<Integer> parts,
        BackupProcessTask task
    ) throws IgniteCheckedException {
        if (!(cctx.database() instanceof GridCacheDatabaseSharedManager) || parts == null || parts.isEmpty())
            return;

        final NavigableSet<GroupPartitionId> grpPartIdSet = parts.stream()
            .map(p -> new GroupPartitionId(grpId, p))
            .collect(Collectors.toCollection(TreeSet::new));

        // Init stores if not created yet.
        initTemporaryStores(grpPartIdSet);

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)cctx.database();

        DbCheckpointListener dbLsnr = null;

        final BackupContext backupCtx = new BackupContext();

        try {
            dbLsnr = new DbCheckpointListener() {
                @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {
                    // Start tracking writes over remaining parts only from the next checkpoint.
                    if (backupCtx.inited.get() && backupCtx.tracked.compareAndSet(false, true)) {
                        // Safe iteration over copy-on-write collection.
                        CopyOnWriteArraySet<GroupPartitionId> leftParts = backupCtx.remainPartIds;

                        for (GroupPartitionId key : leftParts) {
                            // Start track.
                            AtomicInteger cnt = trackMap.putIfAbsent(key, new AtomicInteger(1));

                            if (cnt != null)
                                cnt.incrementAndGet();

                            // Update offsets.
                            backupCtx.deltaOffsetMap.put(key, pageSize * backupStores.get(key).writtenPagesCount());
                        }
                    }
                }

                @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
                    // Will skip the other #onCheckpointBegin() checkpoint. We should wait for the next
                    // checkpoint and if it occurs must start to track writings of remaining in context partitions.
                    // Suppose there are no store writings between the end of last checkpoint and the start on new one.
                    if (backupCtx.inited.compareAndSet(false, true)) {
                        rwlock.readLock().lock();

                        try {
                            PartitionAllocationMap allocationMap = ctx.partitionStatMap();

                            allocationMap.prepareForSnapshot();

                            U.log(log, "Total allocated: " + allocationMap.size());

                            backupCtx.idx = idx;

                            for (GroupPartitionId key : grpPartIdSet) {
                                PagesAllocationRange allocRange = allocationMap.get(key);

                                assert allocRange != null : key;

                                backupCtx.partAllocatedPages.put(key, pageSize * allocRange.getCurrAllocatedPageCnt());

                                // Set offsets with default zero values.
                                backupCtx.deltaOffsetMap.put(key, 0);
                            }

                            backupCtx.remainPartIds = new CopyOnWriteArraySet<>(grpPartIdSet);
                        }
                        finally {
                            rwlock.readLock().unlock();
                        }
                    }
                }
            };

            dbMgr.addCheckpointListener(dbLsnr);

            CheckpointFuture cpFut = dbMgr.wakeupForCheckpointOperation(
                new SnapshotOperationAdapter() {
                    @Override public Set<Integer> cacheGroupIds() {
                        return new HashSet<>(Collections.singletonList(grpId));
                    }
                },
                String.format(BACKUP_CP_REASON, idx, grpId, S.compact(parts))
            );

            A.notNull(cpFut, "Checkpoint thread is not running.");

            cpFut.finishFuture().listen(f -> {
                assert backupCtx.inited.get() : "Backup context must be initialized: " + backupCtx;
            });

            cpFut.finishFuture().get();

            U.log(log, "Start snapshot operation over files [grpId=" + grpId + ", parts=" + S.compact(parts) +
                ", context=" + backupCtx + ']');

            // Use sync mode to execute provided task over partitons and corresponding deltas.
            for (GroupPartitionId grpPartId : grpPartIdSet) {
                IgniteCheckedException pageErr = pageTrackErrors.get(grpPartId);

                if (pageErr != null)
                    throw pageErr;

                final CacheConfiguration grpCfg = cctx.cache()
                    .cacheGroup(grpPartId.getGroupId())
                    .config();

                final long size = backupCtx.partAllocatedPages.get(grpPartId);


                task.handlePartition(grpPartId,
                    resolvePartitionFileCfg(grpCfg, grpPartId.getPartitionId()),
                    0,
                    size);

                // Stop page delta tracking for particular pair id.
                ofNullable(trackMap.get(grpPartId))
                    .ifPresent(AtomicInteger::decrementAndGet);

                U.log(log, "Partition file handled [pairId" + grpPartId + ']');

                final Map<GroupPartitionId, Integer> offsets = backupCtx.deltaOffsetMap;
                final int deltaOffset = offsets.get(grpPartId);
                final long deltaSize = backupStores.get(grpPartId).writtenPagesCount() * pageSize;

                task.handleDelta(grpPartId,
                    resolvePartitionDeltaFileCfg(grpCfg, grpPartId.getPartitionId()),
                    deltaOffset,
                    deltaSize);

                // Finish partition backup task.
                backupCtx.remainPartIds.remove(grpPartId);

                U.log(log, "Partition delta handled [pairId" + grpPartId + ']');
            }
        }
        catch (Exception e) {
            U.error(log, "An error occured while handling partition files.", e);

            for (GroupPartitionId key : grpPartIdSet) {
                AtomicInteger keyCnt = trackMap.get(key);

                if (keyCnt != null && (keyCnt.decrementAndGet() == 0))
                    U.closeQuiet(backupStores.get(key));
            }

            throw new IgniteCheckedException(e);
        }
        finally {
            dbMgr.removeCheckpointListener(dbLsnr);
        }
    }

    /** {@inheritDoc} */
    @Override public void handleWritePageStore(GroupPartitionId pairId, PageStore store, long pageId) {
        AtomicInteger trackCnt = trackMap.get(pairId);

        if (trackCnt == null || trackCnt.get() <= 0)
            return;

        final ByteBuffer tmpPageBuff = threadPageBuff.get();

        tmpPageBuff.clear();

        try {
            store.read(pageId, tmpPageBuff, true);

            tmpPageBuff.flip();

            // We can read a page with zero bytes as it isn't exist in the store (e.g. on first write request).
            // Check the buffer contains only zero bytes and exit.
            if (isNewPage(tmpPageBuff))
                return;

            TemporaryStore tempStore = backupStores.get(pairId);

            assert tempStore != null;

            tempStore.write(pageId, tmpPageBuff);

            tmpPageBuff.clear();
        }
        catch (IgniteDataIntegrityViolationException e) {
            // Page can be readed with zero bytes only.
            U.warn(log, "Ignore integrity violation checks [pairId=" + pairId + ", pageId=" + pageId + "]. " +
                "Error message: " + e.getMessage());
        }
        catch (Exception e) {
            U.error(log, "An error occured in the process of page backup " +
                "[pairId=" + pairId + ", pageId=" + pageId + ']');

            pageTrackErrors.putIfAbsent(pairId,
                new IgniteCheckedException("Partition backup processing error [pageId=" + pageId + ']', e));
        }
    }

    /**
     * @param buff Input array to check.
     * @return {@code True} if contains only zero bytes.
     */
    private boolean isNewPage(ByteBuffer buff) {
        assert buff.position() == 0 : buff.position();
        assert buff.limit() == pageSize : buff.limit();

        byte[] array = threadTempArr.get();

        buff.get(array);

        buff.rewind();

        int sum = 0;

        for (byte b : array)
            sum |= b;

        return sum == 0;
    }

    /** {@inheritDoc} */
    @Override public void initTemporaryStores(Set<GroupPartitionId> grpPartIdSet) throws IgniteCheckedException {
        for (GroupPartitionId grpPartId : grpPartIdSet) {
            CacheConfiguration ccfg = cctx.cache().cacheGroup(grpPartId.getGroupId()).config();

            // Create cache temporary directory if not.
            File tempGroupDir = U.resolveWorkDirectory(backupWorkDir.getAbsolutePath(), cacheDirName(ccfg), false);

            U.ensureDirectory(tempGroupDir, "temporary directory for grpId: " + grpPartId.getGroupId(), log);

            backupStores.putIfAbsent(grpPartId,
                new FileTemporaryStore(getPartionDeltaFile(tempGroupDir,
                    grpPartId.getPartitionId()),
                    ioFactory,
                    pageSize));
        }
    }

    /** */
    public void setThreadPageBuff(final ThreadLocal<ByteBuffer> buf) {
        threadPageBuff = buf;
    }

    /** */
    private static class BackupContext {
        /** */
        private final AtomicBoolean inited = new AtomicBoolean();

        /** */
        private final AtomicBoolean tracked = new AtomicBoolean();

        /** Unique identifier of backup process. */
        private int idx;

        /**
         * The length of partition file sizes up to each cache partiton file.
         * Partition has value greater than zero only for OWNING state partitons.
         */
        private Map<GroupPartitionId, Integer> partAllocatedPages = new HashMap<>();

        /** The offset from which reading of delta partition file should be started. */
        private ConcurrentMap<GroupPartitionId, Integer> deltaOffsetMap = new ConcurrentHashMap<>();

        /** Left partitions to be processed. */
        private CopyOnWriteArraySet<GroupPartitionId> remainPartIds;

        /** {@inheritDoc} */
        @Override public String toString() {
            return "BackupContext {" +
                "inited=" + inited +
                ", tracked=" + tracked +
                ", idx=" + idx +
                ", partAllocatedPages=" + partAllocatedPages +
                ", deltaOffsetMap=" + deltaOffsetMap +
                ", remainPartIds=" + remainPartIds +
                '}';
        }
    }
}
