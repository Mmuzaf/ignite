/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointProgress;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.relativeStoragePath;

/**
 *
 */
public class IgniteSnapshotManagerSelfTest extends GridCommonAbstractTest {
    /** */
    private static final FileIOFactory DFLT_IO_FACTORY = new RandomAccessFileIOFactory();

    /** */
    private static final String SNAPSHOT_NAME = "testSnapshot";

    /** */
    private static final int CACHE_PARTS_COUNT = 8;

    /** */
    private static final int PAGE_SIZE = 1024;

    /** */
    private static final int CACHE_KEYS_RANGE = 1024;

    /** */
    private static final DataStorageConfiguration memCfg = new DataStorageConfiguration()
        .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(100L * 1024 * 1024)
            .setPersistenceEnabled(true))
        .setCheckpointFrequency(3000)
        .setPageSize(PAGE_SIZE)
        .setWalMode(WALMode.LOG_ONLY);

    /** */
    private CacheConfiguration<Integer, Integer> defaultCacheCfg =
        new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false)
                .setPartitions(CACHE_PARTS_COUNT));

    /**
     * Calculate CRC for all partition files of specified cache.
     *
     * @param cacheDir Cache directory to iterate over partition files.
     * @return The map of [fileName, checksum].
     * @throws IgniteCheckedException If fails.
     */
    private static Map<String, Integer> calculateCRC32Partitions(File cacheDir) throws IgniteCheckedException {
        assert cacheDir.isDirectory() : cacheDir.getAbsolutePath();

        Map<String, Integer> result = new HashMap<>();

        try {
            try (DirectoryStream<Path> partFiles = newDirectoryStream(cacheDir.toPath(),
                p -> p.toFile().getName().startsWith(PART_FILE_PREFIX) && p.toFile().getName().endsWith(FILE_SUFFIX))
            ) {
                for (Path path : partFiles)
                    result.put(path.toFile().getName(), FastCrc.calcCrc(path.toFile()));
            }

            return result;
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** */
    @Before
    public void beforeTestSnapshot() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    @After
    public void afterTestSnapshot() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setDataStorageConfiguration(memCfg)
            .setCacheConfiguration(defaultCacheCfg);
    }

    /**
     *
     */
    @Test
    public void testSnapshotLocalPartitions() throws Exception {
        // Start grid node with data before each test.
        IgniteEx ig = startGridWithCache(defaultCacheCfg, CACHE_KEYS_RANGE);

        for (int i = CACHE_KEYS_RANGE; i < 2048; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, i);

        try (IgniteDataStreamer<Integer, TestOrderItem> ds = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 2048; i++)
                ds.addData(i, new TestOrderItem(i, i));
        }

        try (IgniteDataStreamer<Integer, TestOrderItem> ds = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 2048; i++)
                ds.addData(i, new TestOrderItem(i, i) {
                    @Override public String toString() {
                        return "_" + super.toString();
                    }
                });
        }

        IgniteSnapshotManager mgr = ig.context()
            .cache()
            .context()
            .snapshotMgr();

        IgniteInternalFuture<?> snpFut = mgr.createLocalSnapshot(SNAPSHOT_NAME,
            Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME)));

        snpFut.get();

        File cacheWorkDir = ((FilePageStoreManager)ig.context()
            .cache()
            .context()
            .pageStore())
            .cacheWorkDir(defaultCacheCfg);

        stopGrid(ig.name());

        // Calculate CRCs
        final Map<String, Integer> origParts = calculateCRC32Partitions(cacheWorkDir);

        String nodePath = relativeStoragePath(ig.context().cache().context());

        final Map<String, Integer> bakcupCRCs = calculateCRC32Partitions(
            Paths.get(mgr.snapshotLocalDir(SNAPSHOT_NAME).getPath(), nodePath, cacheDirName(defaultCacheCfg)).toFile()
        );

        assertEquals("Partiton must have the same CRC after shapshot and after merge", origParts, bakcupCRCs);

        File snpWorkDir = mgr.snapshotTempDir();

        assertEquals("Snapshot working directory must be cleand after usage", 0, snpWorkDir.listFiles().length);
    }

    /**
     *
     */
    @Test
    public void testSnapshotLocalPartitionsNextCpStarted() throws Exception {
        final int value_multiplier = 2;
        CountDownLatch slowCopy = new CountDownLatch(1);

        IgniteEx ig = startGridWithCache(defaultCacheCfg.setAffinity(new ZeroPartitionAffinityFunction()
            .setPartitions(CACHE_PARTS_COUNT)), CACHE_KEYS_RANGE);

        GridIntList ints = new GridIntList(IntStream.range(0, CACHE_PARTS_COUNT - 1).toArray());
        ints.add(PageIdAllocator.INDEX_PARTITION);

        Map<Integer, GridIntList> parts = new HashMap<>();

        parts.put(CU.cacheId(DEFAULT_CACHE_NAME), ints);

        IgniteSnapshotManager mgr = ig.context()
            .cache()
            .context()
            .snapshotMgr();

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)ig.context()
            .cache()
            .context()
            .database();

        File cpDir = dbMgr.checkpointDirectory();
        File walDir = ((FileWriteAheadLogManager) ig.context().cache().context().wal()).walWorkDir();

        // Change data before backup
        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, value_multiplier * i);

        File snapshotDir0 = mgr.snapshotLocalDir(SNAPSHOT_NAME);

        IgniteInternalFuture<?> snpFut = mgr
            .runLocalSnapshotTask(SNAPSHOT_NAME,
                ig.localNode().id(),
                parts,
                mgr.snapshotExecutorService(),
                new DeleagateSnapshotFileSender(log, mgr.localSnapshotSender(snapshotDir0)) {
                    @Override
                    public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long length) {
                        try {
                            if (pair.getPartitionId() == 0)
                                U.await(slowCopy);

                            delegate.sendPart0(part, cacheDirName, pair, length);
                        }
                        catch (IgniteInterruptedCheckedException e) {
                            throw new IgniteException(e);
                        }
                    }
                });

        dbMgr.forceCheckpoint("snapshot is ready to be created")
            .futureFor(CheckpointState.MARKER_STORED_TO_DISK)
            .get();

        // Change data after backup
        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, 3 * i);

        // Backup on the next checkpoint must copy page before write it to partition
        CheckpointProgress cpFut = ig.context()
            .cache()
            .context()
            .database()
            .forceCheckpoint("second cp");

        cpFut.futureFor(CheckpointState.FINISHED).get();

        slowCopy.countDown();

        snpFut.get();

        // Now can stop the node and check created backups.

        stopGrid(0);

        IgniteUtils.delete(cpDir);
        IgniteUtils.delete(walDir);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0))
            .setWorkDirectory(mgr.snapshotLocalDir(SNAPSHOT_NAME).getAbsolutePath());

        IgniteEx ig2 = startGrid(cfg);

        ig2.cluster().active(true);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++) {
            assertEquals("snapshot data consistency violation [key=" + i + ']',
                i * value_multiplier, ig2.cache(DEFAULT_CACHE_NAME).get(i));
        }
    }

    /**
     *
     */
    @Test(expected = IgniteCheckedException.class)
    public void testSnapshotLocalPartitionNotEnoughSpace() throws Exception {
        final AtomicInteger throwCntr = new AtomicInteger();

        IgniteEx ig = startGridWithCache(defaultCacheCfg.setAffinity(new ZeroPartitionAffinityFunction()
            .setPartitions(CACHE_PARTS_COUNT)), CACHE_KEYS_RANGE);

        // Change data after backup
        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, 2 * i);

        IgniteSnapshotManager mgr = ig.context()
            .cache()
            .context()
            .snapshotMgr();

        mgr.ioFactory(new FileIOFactory() {
            @Override public FileIO create(File file, OpenOption... modes) throws IOException {
                FileIO fileIo = DFLT_IO_FACTORY.create(file, modes);

                if (file.getName().equals(IgniteSnapshotManager.getPartitionDeltaFileName(0)))
                    return new FileIODecorator(fileIo) {
                        @Override public int writeFully(ByteBuffer srcBuf) throws IOException {
                            if (throwCntr.incrementAndGet() == 3)
                                throw new IOException("Test exception. Not enough space.");

                            return super.writeFully(srcBuf);
                        }
                    };

                return fileIo;
            }
        });

        IgniteInternalFuture<?> snpFut = mgr.createLocalSnapshot(SNAPSHOT_NAME,
            Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME)));

        snpFut.get();
    }

    /**
     *
     */
    @Test(expected = IgniteCheckedException.class)
    public void testSnapshotCreateLocalCopyPartitionFail() throws Exception {
        IgniteEx ig = startGridWithCache(defaultCacheCfg, CACHE_KEYS_RANGE);

        Map<Integer, GridIntList> parts = new HashMap<>();

        parts.computeIfAbsent(CU.cacheId(DEFAULT_CACHE_NAME), c -> new GridIntList(1))
            .add(0);

        IgniteSnapshotManager mgr = ig.context()
            .cache()
            .context()
            .snapshotMgr();

        File snpDir0 = mgr.snapshotLocalDir(SNAPSHOT_NAME);

        IgniteInternalFuture<?> fut = mgr.runLocalSnapshotTask(SNAPSHOT_NAME,
            ig.localNode().id(),
            parts,
            mgr.snapshotExecutorService(),
            new DeleagateSnapshotFileSender(log, mgr.localSnapshotSender(snpDir0)) {
                @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long length) {
                    if (pair.getPartitionId() == 0)
                        throw new IgniteException("Test. Fail to copy partition: " + pair);

                    delegate.sendPart0(part, cacheDirName, pair, length);
                }
            });

        fut.get();
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testSnapshotRemotePartitions() throws Exception {
        IgniteEx ig0 = startGrids(2);

        ig0.cluster().active(true);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig0.cache(DEFAULT_CACHE_NAME).put(i, i);

        CheckpointProgress cpFut = ig0.context()
            .cache()
            .context()
            .database()
            .forceCheckpoint("the next one");

        cpFut.futureFor(CheckpointState.FINISHED).get();

        IgniteSnapshotManager mgr0 = ig0.context()
            .cache()
            .context()
            .snapshotMgr();

        final CountDownLatch cancelLatch = new CountDownLatch(1);

        mgr0.addSnapshotListener(new SnapshotListener() {
            @Override public void onPartition(UUID rmtNodeId, File part, int grpId, int partId) {
                log.info("Snapshot partition received successfully [rmtNodeId=" + rmtNodeId +
                    ", part=" + part.getAbsolutePath() + ", grpId=" + grpId + ", partId=" + partId + ']');

                cancelLatch.countDown();
            }

            @Override public void onEnd(UUID rmtNodeId) {
                log.info("Snapshot created successfully [rmtNodeId=" + rmtNodeId + ']');
            }

            @Override public void onException(UUID rmtNodeId, Throwable t) {
                fail("Exception must not be thrown [rmtNodeId=" + rmtNodeId + ", t=" + t);
            }
        });

        UUID rmtNodeId = grid(1).localNode().id();

        // Snapshot must be taken on node1 and transmitted to node0.
        IgniteInternalFuture<?> fut = mgr0.createRemoteSnapshot(rmtNodeId,
            owningParts(ig0, new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))), rmtNodeId));

        cancelLatch.await();

        fut.cancel();

        IgniteInternalFuture<?> fut2 = mgr0.createRemoteSnapshot(rmtNodeId,
            owningParts(ig0, new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))), rmtNodeId));

        fut2.get();
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testSnapshotRemoteOnBothNodes() throws Exception {
        IgniteEx ig0 = startGrids(2);

        ig0.cluster().active(true);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig0.cache(DEFAULT_CACHE_NAME).put(i, i);

        CheckpointProgress cpFut = ig0.context()
            .cache()
            .context()
            .database()
            .forceCheckpoint("the next one");

        cpFut.futureFor(CheckpointState.FINISHED).get();

        IgniteSnapshotManager mgr0 = ig0.context()
            .cache()
            .context()
            .snapshotMgr();

        IgniteSnapshotManager mgr1 = grid(1).context()
            .cache()
            .context()
            .snapshotMgr();

        UUID node0 = grid(0).localNode().id();
        UUID node1 = grid(1).localNode().id();

        // Snapshot must be taken on node1 and transmitted to node0.
        IgniteInternalFuture<?> futFrom1To0 = mgr0.createRemoteSnapshot(node1,
            owningParts(ig0, new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))), node1));

        IgniteInternalFuture<?> futFrom0To1 = mgr1.createRemoteSnapshot(node0,
            owningParts(grid(1), new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))), node0));

        futFrom0To1.get();
        futFrom1To0.get();
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = ClusterTopologyCheckedException.class)
    public void testRemoteSnapshotRequestedNodeLeft() throws Exception {
        IgniteEx ig0 = startGridWithCache(defaultCacheCfg, CACHE_KEYS_RANGE);
        IgniteEx ig1 = startGrid(1);

        ig0.cluster().setBaselineTopology(ig0.cluster().forServers().nodes());

        awaitPartitionMapExchange();

        CountDownLatch hold = new CountDownLatch(1);

        ((GridCacheDatabaseSharedManager)ig1.context()
            .cache()
            .context()
            .database())
            .waitForCheckpoint("Snapshot before request", f -> {
                try {
                    // Listener will be exectuted inside the checkpoint thead.
                    U.await(hold);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            });

        UUID rmtNodeId = ig1.localNode().id();

        ig0.context()
            .cache()
            .context()
            .snapshotMgr()
            .createRemoteSnapshot(rmtNodeId,
            owningParts(ig0,
                new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))),
                rmtNodeId));

        IgniteInternalFuture[] futs = new IgniteInternalFuture[1];

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                IgniteInternalFuture<Boolean> snpFut = ig1.context()
                    .cache()
                    .context()
                    .snapshotMgr()
                    .snapshotRemoteRequest(ig0.localNode().id());

                if (snpFut == null)
                    return false;
                else
                    futs[0] = snpFut;

                return true;
            }
        }, 5_000L);

        stopGrid(0);

        hold.countDown();

        futs[0].get();
    }

    /**
     * <pre>
     * 1. Start 2 nodes.
     * 2. Request snapshot from 2-nd node
     * 3. Block snapshot-request message.
     * 4. Start 3-rd node and change BLT.
     * 5. Stop 3-rd node and change BLT.
     * 6. 2-nd node now have MOVING partitions to be preloaded.
     * 7. Release snapshot-request message.
     * 8. Should get an error of snapshot creation since MOVING partitions cannot be snapshotted.
     * </pre>
     *
     * @throws Exception If fails.
     */
    @Test(expected = IgniteCheckedException.class)
    public void testRemoteOutdatedSnapshot() throws Exception {
        IgniteEx ig0 = startGrids(2);

        ig0.cluster().active(true);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig0.cache(DEFAULT_CACHE_NAME).put(i, i);

        awaitPartitionMapExchange();

        for (int i = 0; i < 2; i++) {
            grid(i).
                context()
                .cache()
                .context()
                .database()
                .forceCheckpoint("the next one")
                .futureFor(CheckpointState.FINISHED)
                .get();
        }

        TestRecordingCommunicationSpi.spi(ig0)
            .blockMessages((node, msg) -> msg instanceof SnapshotRequestMessage);

        UUID rmtNodeId = grid(1).localNode().id();

        IgniteSnapshotManager mgr0 = ig0.context()
            .cache()
            .context()
            .snapshotMgr();

        // Snapshot must be taken on node1 and transmitted to node0.
        IgniteInternalFuture<?> snpFut = mgr0.createRemoteSnapshot(rmtNodeId,
            owningParts(ig0, new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))), rmtNodeId));

        TestRecordingCommunicationSpi.spi(ig0)
            .waitForBlocked();

        startGrid(2);

        ig0.cluster().setBaselineTopology(ig0.cluster().forServers().nodes());

        awaitPartitionMapExchange();

        stopGrid(2);

        TestRecordingCommunicationSpi.spi(grid(1))
            .blockMessages((node, msg) ->  msg instanceof GridDhtPartitionDemandMessage);

        ig0.cluster().setBaselineTopology(ig0.cluster().forServers().nodes());

        TestRecordingCommunicationSpi.spi(ig0)
            .stopBlock(true, obj -> obj.get2().message() instanceof SnapshotRequestMessage);

        snpFut.get();
    }

    /**
     * @throws Exception If fails.
     */
    @Test(expected = IgniteCheckedException.class)
    public void testLocalSnapshotOnCacheStopped() throws Exception {
        IgniteEx ig = startGridWithCache(defaultCacheCfg, CACHE_KEYS_RANGE);

        startGrid(1);

        ig.cluster().active(true);

        awaitPartitionMapExchange();

        GridIntList ints = new GridIntList(IntStream.range(0, CACHE_PARTS_COUNT - 1).toArray());
        ints.add(PageIdAllocator.INDEX_PARTITION);

        Map<Integer, GridIntList> parts = new HashMap<>();
        parts.put(CU.cacheId(DEFAULT_CACHE_NAME), ints);

        IgniteSnapshotManager mgr = ig.context()
            .cache()
            .context()
            .snapshotMgr();

        CountDownLatch cpLatch = new CountDownLatch(1);

        File snapshotDir0 = mgr.snapshotLocalDir(SNAPSHOT_NAME);

        IgniteInternalFuture<?> snpFut = mgr
            .runLocalSnapshotTask(SNAPSHOT_NAME,
                ig.localNode().id(),
                parts,
                mgr.snapshotExecutorService(),
                new DeleagateSnapshotFileSender(log, mgr.localSnapshotSender(snapshotDir0)) {
                    @Override
                    public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long length) {
                        try {
                            U.await(cpLatch);

                            delegate.sendPart0(part, cacheDirName, pair, length);
                        }
                        catch (IgniteInterruptedCheckedException e) {
                            throw new IgniteException(e);
                        }
                    }
                });

        IgniteCache<?, ?> cache = ig.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.destroy();

        cpLatch.countDown();

        snpFut.get(5_000, TimeUnit.MILLISECONDS);
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testClusterSnapshot() throws Exception {

    }

    /**
     * @param src Source node to calculate.
     * @param grps Groups to collect owning parts.
     * @param rmtNodeId Remote node id.
     * @return Map of collected parts.
     */
    private static Map<Integer, Set<Integer>> owningParts(IgniteEx src, Set<Integer> grps, UUID rmtNodeId) {
        Map<Integer, Set<Integer>> result = new HashMap<>();

        for (Integer grpId : grps) {
            Set<Integer> parts = src.context()
                .cache()
                .cacheGroup(grpId)
                .topology()
                .partitions(rmtNodeId)
                .entrySet()
                .stream()
                .filter(p -> p.getValue() == GridDhtPartitionState.OWNING)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

            result.put(grpId, parts);
        }

        return result;
    }

    /**
     * @param ccfg Default cache configuration.
     * @return Ignite instance.
     * @throws Exception If fails.
     */
    private IgniteEx startGridWithCache(CacheConfiguration<Integer, Integer> ccfg, int range) throws Exception {
        defaultCacheCfg = ccfg;

        // Start grid node with data before each test.
        IgniteEx ig = startGrid(0);

        ig.cluster().baselineAutoAdjustEnabled(false);
        ig.cluster().active(true);

        for (int i = 0; i < range; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, i);

        CheckpointProgress cpFut = ig.context()
            .cache()
            .context()
            .database()
            .forceCheckpoint("the next one");

        cpFut.futureFor(CheckpointState.FINISHED).get();

        return ig;
    }

    /**
     *
     */
    private static class ZeroPartitionAffinityFunction extends RendezvousAffinityFunction {
        @Override public int partition(Object key) {
            return 0;
        }
    }

    /**
     *
     */
    private static class DeleagateSnapshotFileSender extends SnapshotFileSender {
        /** Delegate call to. */
        protected final SnapshotFileSender delegate;

        /**
         * @param delegate Delegate call to.
         */
        public DeleagateSnapshotFileSender(IgniteLogger log, SnapshotFileSender delegate) {
            super(log);

            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public void sendCacheConfig0(File ccfg, String cacheDirName) {
            delegate.sendCacheConfig(ccfg, cacheDirName);
        }

        /** {@inheritDoc} */
        @Override public void sendMarshallerMeta0(List<Map<Integer, MappedName>> mappings) {
            delegate.sendMarshallerMeta(mappings);
        }

        /** {@inheritDoc} */
        @Override public void sendBinaryMeta0(Map<Integer, BinaryType> types) {
            delegate.sendBinaryMeta(types);
        }

        /** {@inheritDoc} */
        @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long length) {
            delegate.sendPart(part, cacheDirName, pair, length);
        }

        /** {@inheritDoc} */
        @Override public void sendDelta0(File delta, String cacheDirName, GroupPartitionId pair) {
            delegate.sendDelta(delta, cacheDirName, pair);
        }

        /** {@inheritDoc} */
        @Override public void close0(Throwable th) {
            delegate.close(th);
        }
    }

    /**
     *
     */
    private static class TestOrderItem implements Serializable {
        /** Order key. */
        private final int key;

        /** Order value. */
        private final int value;

        public TestOrderItem(int key, int value) {
            this.key = key;
            this.value = value;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestOrderItem item = (TestOrderItem)o;

            return key == item.key &&
                value == item.value;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(key, value);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestOrderItem [key=" + key + ", value=" + value + ']';
        }
    }
}
