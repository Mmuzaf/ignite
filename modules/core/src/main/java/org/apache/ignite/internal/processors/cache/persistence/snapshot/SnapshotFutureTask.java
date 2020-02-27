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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.store.PageWriteListener;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PagesAllocationRange;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteThrowableRunner;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.getPartionDeltaFile;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.relativeNodePath;

/**
 *
 */
class SnapshotFutureTask extends GridFutureAdapter<Boolean> implements DbCheckpointListener {
    /** Shared context. */
    private final GridCacheSharedContext<?, ?> cctx;

    /** Ignite logger */
    private final IgniteLogger log;

    /** Node id which cause snapshot operation. */
    private final UUID srcNodeId;

    /** Unique identifier of snapshot process. */
    private final String snpName;

    /** Snapshot working directory on file system. */
    private final File tmpTaskWorkDir;

    /**
     * The length of file size per each cache partiton file.
     * Partition has value greater than zero only for partitons in OWNING state.
     * Information collected under checkpoint write lock.
     */
    private final Map<GroupPartitionId, Long> partFileLengths = new HashMap<>();

    /**
     * Map of partitions to snapshot and theirs corresponding delta PageStores.
     * Writers are pinned to the snapshot context due to controlling partition
     * processing supplier.
     */
    private final Map<GroupPartitionId, PageStoreSerialWriter> partDeltaWriters = new HashMap<>();

    /** Snapshot data sender. */
    @GridToStringExclude
    private final SnapshotFileSender snpSndr;

    /** Collection of partition to be snapshotted. */
    private final List<GroupPartitionId> parts = new ArrayList<>();

    /** Checkpoint end future. */
    private final CompletableFuture<Boolean> cpEndFut = new CompletableFuture<>();

    /** Future to wait until checkpoint mark pahse will be finished and snapshot tasks scheduled. */
    private final GridFutureAdapter<Void> startedFut = new GridFutureAdapter<>();

    /** Absolute snapshot storage path. */
    private File tmpSnpDir;

    /** {@code true} if operation has been cancelled. */
    private volatile boolean cancelled;

    /** An exception which has been ocurred during snapshot processing. */
    private final AtomicReference<Throwable> err = new AtomicReference<>();

    /** Flag indicates the task must be interrupted. */
    private final BooleanSupplier stopping = () -> cancelled || err.get() != null;

    /**
     * @param e Finished snapshot tosk future with particular exception.
     */
    public SnapshotFutureTask(IgniteCheckedException e) {
        A.notNull(e, "Exception for a finished snapshot task must be not null");

        cctx = null;
        log = null;
        snpName = null;
        srcNodeId = null;
        tmpTaskWorkDir = null;
        snpSndr = null;

        err.set(e);
        startedFut.onDone(e);
        onDone(e);
    }

    /**
     * @param snpName Unique identifier of snapshot task.
     * @param ioFactory Factory to working with delta as file storage.
     */
    public SnapshotFutureTask(
        GridCacheSharedContext<?, ?> cctx,
        UUID srcNodeId,
        String snpName,
        File tmpWorkDir,
        FileIOFactory ioFactory,
        SnapshotFileSender snpSndr,
        Map<Integer, GridIntList> parts
    ) {
        A.notNull(snpName, "Snapshot name cannot be empty or null");
        A.notNull(snpSndr, "Snapshot sender which handles execution tasks must be not null");
        A.notNull(snpSndr.executor(), "Executor service must be not null");

        this.cctx = cctx;
        this.log = cctx.logger(SnapshotFutureTask.class);
        this.snpName = snpName;
        this.srcNodeId = srcNodeId;
        this.tmpTaskWorkDir = new File(tmpWorkDir, snpName);
        this.snpSndr = snpSndr;

        for (Map.Entry<Integer, GridIntList> e : parts.entrySet()) {
            GridIntIterator iter = e.getValue().iterator();

            while (iter.hasNext())
                this.parts.add(new GroupPartitionId(e.getKey(), iter.next()));
        }

        try {
            tmpSnpDir = U.resolveWorkDirectory(tmpTaskWorkDir.getAbsolutePath(),
                relativeNodePath(cctx.kernalContext().pdsFolderResolver().resolveFolders()),
                false);

            this.snpSndr.init();

            Map<Integer, File> dirs = new HashMap<>();

            for (Integer grpId : parts.keySet()) {
                CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

                if (gctx == null)
                    throw new IgniteCheckedException("Cache group context has not found. Cache group is stopped: " + grpId);

                if (!CU.isPersistentCache(gctx.config(), cctx.kernalContext().config().getDataStorageConfiguration()))
                    throw new IgniteCheckedException("In-memory cache groups are not allowed to be snapshotted: " + grpId);

                if (gctx.config().isEncryptionEnabled())
                    throw new IgniteCheckedException("Encrypted cache groups are note allowed to be snapshotted: " + grpId);

                // Create cache snapshot directory if not.
                File grpDir = U.resolveWorkDirectory(tmpSnpDir.getAbsolutePath(),
                    cacheDirName(gctx.config()), false);

                U.ensureDirectory(grpDir,
                    "snapshot directory for cache group: " + gctx.groupId(),
                    null);

                dirs.put(grpId, grpDir);
            }

            CompletableFuture<Boolean> cpEndFut0 = cpEndFut;

            for (GroupPartitionId pair : this.parts) {
                PageStore store = ((FilePageStoreManager)cctx.pageStore()).getStore(pair.getGroupId(),
                    pair.getPartitionId());

                partDeltaWriters.put(pair,
                    new PageStoreSerialWriter(log,
                        store,
                        () -> cpEndFut0.isDone() && !cpEndFut0.isCompletedExceptionally(),
                        stopping,
                        this::acceptException,
                        getPartionDeltaFile(dirs.get(pair.getGroupId()), pair.getPartitionId()),
                        ioFactory,
                        cctx.kernalContext()
                            .config()
                            .getDataStorageConfiguration()
                            .getPageSize()));
            }

            if (log.isInfoEnabled()) {
                log.info("Snapshot task has been created [sctx=" + this +
                    ", topVer=" + cctx.discovery().topologyVersionEx() + ']');
            }
        }
        catch (IgniteCheckedException e) {
            acceptException(e);
        }
    }

    /**
     * @return Node id which triggers this operation..
     */
    public UUID sourceNodeId() {
        return srcNodeId;
    }

    /**
     * @return Type of snapshot operation.
     */
    public Class<? extends SnapshotFileSender> type() {
        return snpSndr.getClass();
    }

    /**
     * @return List of partitions to be processed.
     */
    public List<GroupPartitionId> partitions() {
        return parts;
    }

    /**
     * @param th An exception which occurred during snapshot processing.
     */
    public void acceptException(Throwable th) {
        if (th == null)
            return;

        if (err.compareAndSet(null, th))
            closeAsync();

        startedFut.onDone(th);

        log.error("Exception occurred during snapshot operation", th);
    }

    /**
     * Close snapshot operation and release resources being used.
     */
    private void close() {
        if (isDone())
            return;

        Throwable err0 = err.get();

        if (onDone(true, err0, cancelled)) {
            for (PageStoreSerialWriter writer : partDeltaWriters.values())
                U.closeQuiet(writer);

            snpSndr.close(err0);

            if (tmpSnpDir != null)
                U.delete(tmpSnpDir);

            // Delete snapshot directory if no other files exists.
            try {
                if (U.fileCount(tmpTaskWorkDir.toPath()) == 0 || err0 != null)
                    U.delete(tmpTaskWorkDir.toPath());
            }
            catch (IOException e) {
                log.error("Snapshot directory doesn't exist [snpName=" + snpName + ", dir=" + tmpTaskWorkDir + ']');
            }

            if (err0 != null)
                startedFut.onDone(err0);
        }
    }

    /**
     * @throws IgniteCheckedException If fails.
     */
    public void awaitStarted() throws IgniteCheckedException {
        startedFut.get();
    }

    /**
     * Initiates snapshot task.
     */
    public void start() {
        if (stopping.getAsBoolean())
            return;

        startedFut.listen(f ->
            ((GridCacheDatabaseSharedManager)cctx.database()).removeCheckpointListener(this)
        );

        // Listener will be removed right after first execution
        ((GridCacheDatabaseSharedManager)cctx.database()).addCheckpointListener(this);

        if (log.isInfoEnabled()) {
            log.info("Snapshot operation is scheduled on local node and will be handled by the checkpoint " +
                "listener [sctx=" + this + ", topVer=" + cctx.discovery().topologyVersionEx() + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public void beforeCheckpointBegin(Context ctx) {
        if (stopping.getAsBoolean())
            return;

        // Gather partitions metainfo for thouse which will be copied.
        ctx.collectPartStat(parts);

        ctx.finishedStateFut().listen(f -> {
            if (f.error() == null)
                cpEndFut.complete(true);
            else
                cpEndFut.completeExceptionally(f.error());
        });
    }

    /** {@inheritDoc} */
    @Override public void onMarkCheckpointBegin(Context ctx) {
        // Write lock is helded. Partition counters has been collected under write lock
        // in another checkpoint listeners.
    }

    /** {@inheritDoc} */
    @Override public void onMarkCheckpointEnd(Context ctx) {
        if (stopping.getAsBoolean())
            return;

        // Under the write lock here. It's safe to add new stores.
        try {
            PartitionAllocationMap allocationMap = ctx.partitionStatMap();

            allocationMap.prepareForSnapshot();

            for (GroupPartitionId pair : parts) {
                PagesAllocationRange allocRange = allocationMap.get(pair);

                GridDhtLocalPartition part = pair.getPartitionId() == INDEX_PARTITION ? null :
                    cctx.cache()
                        .cacheGroup(pair.getGroupId())
                        .topology()
                        .localPartition(pair.getPartitionId());

                // Partition can be reserved.
                // Partition can be MOVING\RENTING states.
                // Index partition will be excluded if not all partition OWNING.
                // There is no data assigned to partition, thus it haven't been created yet.
                assert allocRange != null || part == null || part.state() != GridDhtPartitionState.OWNING :
                    "Partition counters has not been collected " +
                        "[pair=" + pair + ", snpName=" + snpName + ", part=" + part + ']';

                if (allocRange == null) {
                    List<GroupPartitionId> missed = parts.stream()
                        .filter(allocationMap::containsKey)
                        .collect(Collectors.toList());

                    acceptException(new IgniteCheckedException("Snapshot operation cancelled due to " +
                        "not all of requested partitions has OWNING state [missed=" + missed + ']'));

                    break;
                }

                PageStore store = ((FilePageStoreManager)cctx.pageStore()).getStore(pair.getGroupId(), pair.getPartitionId());

                partFileLengths.put(pair, store.size());
                partDeltaWriters.get(pair).init(allocRange.getCurrAllocatedPageCnt());
            }
        }
        catch (IgniteCheckedException e) {
            acceptException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) {
        if (stopping.getAsBoolean())
            return;

        // Snapshot task is now started since checkpoint writelock released.
        if (!startedFut.onDone())
            return;

        // Submit all tasks for partitions and deltas processing.
        List<CompletableFuture<Void>> futs = new ArrayList<>();
        FilePageStoreManager storeMgr = (FilePageStoreManager)cctx.pageStore();

        if (log.isInfoEnabled())
            log.info("Submit partition processings tasks with partition allocated lengths: " + partFileLengths);

        // Process binary meta.
        futs.add(CompletableFuture.runAsync(
            wrapExceptionIfStarted(() ->
                    snpSndr.sendBinaryMeta(cctx.kernalContext()
                        .cacheObjects()
                        .metadata(Collections.emptyList()))),
            snpSndr.executor()));

        // Process marshaller meta.
        futs.add(CompletableFuture.runAsync(
            wrapExceptionIfStarted(() ->
                    snpSndr.sendMarshallerMeta(cctx.kernalContext()
                        .marshallerContext()
                        .getCachedMappings())),
            snpSndr.executor()));

        // Process cache group configuration files.
        parts.stream()
            .map(GroupPartitionId::getGroupId)
            .collect(Collectors.toSet())
            .forEach(grpId ->
                futs.add(CompletableFuture.runAsync(
                        wrapExceptionIfStarted(() -> {
                                CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

                                if (gctx == null) {
                                    throw new IgniteCheckedException("Cache group configuration has not found " +
                                        "due to the cache group is stopped: " + grpId);
                                }

                                List<File> ccfgs = storeMgr.configurationFiles(gctx.config());

                                if (ccfgs == null)
                                    return;

                                for (File ccfg0 : ccfgs)
                                    snpSndr.sendCacheConfig(ccfg0, cacheDirName(gctx.config()));
                            }),
                    snpSndr.executor())
                )
            );

        // Process partitions.
        for (GroupPartitionId pair : parts) {
            CacheGroupContext gctx = cctx.cache().cacheGroup(pair.getGroupId());

            if (gctx == null) {
                acceptException(new IgniteCheckedException("Cache group context has not found " +
                    "due to the cache group is stopped: " + pair));

                break;
            }

            CacheConfiguration<?, ?> ccfg = gctx.config();

            assert ccfg != null : "Cache configuraction cannot be empty on snapshot creation: " + pair;

            String cacheDirName = cacheDirName(ccfg);
            Long partLen = partFileLengths.get(pair);

            CompletableFuture<Void> fut0 = CompletableFuture.runAsync(
                wrapExceptionIfStarted(() -> {
                        snpSndr.sendPart(
                            getPartitionFile(storeMgr.workDir(), cacheDirName, pair.getPartitionId()),
                            cacheDirName,
                            pair,
                            partLen);

                        // Stop partition writer.
                        partDeltaWriters.get(pair).markPartitionProcessed();
                    }),
                snpSndr.executor())
                // Wait for the completion of both futures - checkpoint end, copy partition.
                .runAfterBothAsync(cpEndFut,
                    wrapExceptionIfStarted(() -> {
                            File delta = getPartionDeltaFile(cacheWorkDir(tmpSnpDir, cacheDirName),
                                pair.getPartitionId());

                            snpSndr.sendDelta(delta, cacheDirName, pair);

                            boolean deleted = delta.delete();

                            assert deleted;
                        }),
                    snpSndr.executor());

            futs.add(fut0);
        }

        int futsSize = futs.size();

        CompletableFuture.allOf(futs.toArray(new CompletableFuture[futsSize]))
            .whenComplete((res, t) -> {
                assert t == null : "Excepction must never be thrown since a wrapper is used " +
                    "for each snapshot task: " + t;

                close();
            });
    }

    /**
     * @param exec Runnable task to execute.
     * @return Wrapped task.
     */
    private Runnable wrapExceptionIfStarted(IgniteThrowableRunner exec) {
        return () -> {
            if (stopping.getAsBoolean())
                return;

            try {
                exec.run();
            }
            catch (Throwable t) {
                acceptException(t);
            }
        };
    }

    /**
     * @return Future which will be completed when operations truhly stopped.
     */
    public CompletableFuture<Void> closeAsync() {
        // Execute on SYSTEM_POOL
        return CompletableFuture.runAsync(this::close, cctx.kernalContext().getSystemExecutorService());
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        cancelled = true;

        try {
            closeAsync().get();
        }
        catch (InterruptedException | ExecutionException e) {
            U.error(log, "SnapshotFutureTask cancellation failed", e);

            return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        SnapshotFutureTask ctx = (SnapshotFutureTask)o;

        return snpName.equals(ctx.snpName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(snpName);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotFutureTask.class, this);
    }

    /**
     *
     */
    private static class PageStoreSerialWriter implements PageWriteListener, Closeable {
        /** Ignite logger to use. */
        @GridToStringExclude
        private final IgniteLogger log;

        /** Page store to which current writer is related to. */
        private final PageStore store;

        /** Busy lock to protect write opertions. */
        private final ReadWriteLock lock = new ReentrantReadWriteLock();

        /** Local buffer to perpform copy-on-write operations. */
        private final ThreadLocal<ByteBuffer> localBuff;

        /** {@code true} if need the original page from PageStore instead of given buffer. */
        private final BooleanSupplier checkpointComplete;

        /** {@code true} if snapshot process is stopping or alredy stopped. */
        private final BooleanSupplier interrupt;

        /** Callback to stop snapshot if an error occurred. */
        private final Consumer<Throwable> exConsumer;

        /** IO over the underlying file */
        private volatile FileIO fileIo;

        /** {@code true} if partition file has been copied to external resource. */
        private volatile boolean partProcessed;

        /** {@code true} means current writer is allowed to handle page writes. */
        private volatile boolean inited;

        /**
         * Array of bits. 1 - means pages written, 0 - the otherwise.
         * Size of array can be estimated only under checkpoint write lock.
         */
        private volatile AtomicBitSet pagesWrittenBits;

        /**
         * @param log Ignite logger to use.
         * @param checkpointComplete Checkpoint finish flag.
         * @param pageSize Size of page to use for local buffer.
         * @param cfgFile Configuration file provider.
         * @param factory Factory to produce an IO interface over underlying file.
         */
        public PageStoreSerialWriter(
            IgniteLogger log,
            PageStore store,
            BooleanSupplier checkpointComplete,
            BooleanSupplier interrupt,
            Consumer<Throwable> exConsumer,
            File cfgFile,
            FileIOFactory factory,
            int pageSize
        ) throws IgniteCheckedException {
            assert store != null;

            try {
                fileIo = factory.create(cfgFile);
            }
            catch (IOException e) {
                throw new IgniteCheckedException(e);
            }

            this.checkpointComplete = checkpointComplete;
            this.interrupt = interrupt;
            this.exConsumer = exConsumer;
            this.log = log.getLogger(PageStoreSerialWriter.class);

            localBuff = ThreadLocal.withInitial(() ->
                ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder()));

            this.store = store;

            store.addWriteListener(this);
        }

        /**
         * It is important to init {@link AtomicBitSet} under the checkpoint write-lock.
         * This guarantee us that no pages will be modified and it's safe to init pages list
         * which needs to be processed.
         *
         * @param allocPages Total number of tracking pages.
         */
        public void init(int allocPages) {
            lock.writeLock().lock();

            try {
                pagesWrittenBits = new AtomicBitSet(allocPages);
                inited = true;
            }
            finally {
                lock.writeLock().unlock();
            }
        }

        /**
         * @return {@code true} if writer is stopped and cannot write pages.
         */
        public boolean stopped() {
            return (checkpointComplete.getAsBoolean() && partProcessed) || interrupt.getAsBoolean();
        }

        /**
         * Mark partition has been processed by another thread.
         */
        public void markPartitionProcessed() {
            lock.writeLock().lock();

            try {
                partProcessed = true;
            }
            finally {
                lock.writeLock().unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public void accept(long pageId, ByteBuffer buf) {
            assert buf.position() == 0 : buf.position();
            assert buf.order() == ByteOrder.nativeOrder() : buf.order();

            lock.readLock().lock();

            try {
                if (!inited)
                    return;

                if (stopped())
                    return;

                if (checkpointComplete.getAsBoolean()) {
                    int pageIdx = PageIdUtils.pageIndex(pageId);

                    // Page already written.
                    if (!pagesWrittenBits.touch(pageIdx))
                        return;

                    final ByteBuffer locBuf = localBuff.get();

                    assert locBuf.capacity() == store.getPageSize();

                    locBuf.clear();

                    if (!store.read(pageId, locBuf, true))
                        return;

                    locBuf.flip();

                    writePage0(pageId, locBuf);
                }
                else {
                    // Direct buffre is needs to be written, associated checkpoint not finished yet.
                    writePage0(pageId, buf);
                }
            }
            catch (Throwable ex) {
                exConsumer.accept(ex);
            }
            finally {
                lock.readLock().unlock();
            }
        }

        /**
         * @param pageId Page ID.
         * @param pageBuf Page buffer to write.
         * @throws IOException If page writing failed (IO error occurred).
         */
        private void writePage0(long pageId, ByteBuffer pageBuf) throws IOException {
            assert fileIo != null : "Delta pages storage is not inited: " + this;
            assert pageBuf.position() == 0;
            assert pageBuf.order() == ByteOrder.nativeOrder() : "Page buffer order " + pageBuf.order()
                + " should be same with " + ByteOrder.nativeOrder();

            int crc = PageIO.getCrc(pageBuf);
            int crc32 = FastCrc.calcCrc(pageBuf, pageBuf.limit());

            if (log.isDebugEnabled()) {
                log.debug("onPageWrite [pageId=" + pageId +
                    ", pageIdBuff=" + PageIO.getPageId(pageBuf) +
                    ", fileSize=" + fileIo.size() +
                    ", crcBuff=" + crc32 +
                    ", crcPage=" + crc + ']');
            }

            pageBuf.rewind();

            // Write buffer to the end of the file.
            fileIo.writeFully(pageBuf);
        }

        /** {@inheritDoc} */
        @Override public void close() {
            lock.writeLock().lock();

            try {
                U.closeQuiet(fileIo);

                fileIo = null;

                store.removeWriteListener(this);

                inited = false;
            }
            finally {
                lock.writeLock().unlock();
            }
        }
    }

    /**
     *
     */
    private static class AtomicBitSet {
        /** Container of bits. */
        private final AtomicIntegerArray arr;

        /** Size of array of bits. */
        private final int size;

        /**
         * @param size Size of array.
         */
        public AtomicBitSet(int size) {
            this.size = size;

            arr = new AtomicIntegerArray((size + 31) >>> 5);
        }

        /**
         * @param off Bit position to change.
         * @return {@code true} if bit has been set,
         * {@code false} if bit changed by another thread or out of range.
         */
        public boolean touch(long off) {
            if (off > size)
                return false;

            int bit = 1 << off;
            int bucket = (int)(off >>> 5);

            while (true) {
                int cur = arr.get(bucket);
                int val = cur | bit;

                if (cur == val)
                    return false;

                if (arr.compareAndSet(bucket, cur, val))
                    return true;
            }
        }
    }
}