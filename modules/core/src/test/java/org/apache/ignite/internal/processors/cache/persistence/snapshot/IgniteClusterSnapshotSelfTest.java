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
import java.nio.file.OpenOption;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.FullMessage;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNP_IN_PROGRESS_ERR_MSG;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNP_NODE_STOPPING_ERR_MSG;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.snapshotPath;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.startedBySnapshot;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/**
 * Cluster-wide snapshot test.
 */
public class IgniteClusterSnapshotSelfTest extends AbstractSnapshotSelfTest {
    /** Random instance. */
    private static final Random R = new Random();

    /** Time to wait while rebalance may happen. */
    private static final long REBALANCE_AWAIT_TIME = GridTestUtils.SF.applyLB(10_000, 3_000);

    /** Cache configuration for test. */
    private static CacheConfiguration<Integer, Integer> txCcfg = new CacheConfiguration<Integer, Integer>("txCacheName")
        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
        .setBackups(2)
        .setAffinity(new RendezvousAffinityFunction(false)
            .setPartitions(CACHE_PARTS_COUNT));

    /** {@code true} if node should be started in separate jvm. */
    protected volatile boolean jvm;

    /** @throws Exception If fails. */
    @Before
    @Override public void beforeTestSnapshot() throws Exception {
        super.beforeTestSnapshot();

        jvm = false;
    }

    /**
     * Take snapshot from the whole cluster and check snapshot consistency.
     * Note: Client nodes and server nodes not in baseline topology must not be affected.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testConsistentClusterSnapshotUnderLoad() throws Exception {
        int grids = 3;
        String snpName = "backup23012020";
        AtomicInteger atKey = new AtomicInteger(CACHE_KEYS_RANGE);
        AtomicInteger txKey = new AtomicInteger(CACHE_KEYS_RANGE);

        IgniteEx ignite = startGrids(grids);
        startClientGrid();

        ignite.cluster().baselineAutoAdjustEnabled(false);
        ignite.cluster().state(ACTIVE);

        // Start node not in baseline.
        IgniteEx notBltIgnite = startGrid(grids);
        File locSnpDir = snp(notBltIgnite).snapshotLocalDir(SNAPSHOT_NAME);
        String notBltDirName = folderName(notBltIgnite);

        IgniteCache<Integer, Integer> cache = ignite.createCache(txCcfg);

        for (int idx = 0; idx < CACHE_KEYS_RANGE; idx++) {
            cache.put(txKey.incrementAndGet(), -1);
            ignite.cache(DEFAULT_CACHE_NAME).put(atKey.incrementAndGet(), -1);
        }

        forceCheckpoint();

        CountDownLatch loadLatch = new CountDownLatch(1);

        ignite.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
            /** {@inheritDoc} */
            @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                if (fut.firstEvent().type() != EVT_DISCOVERY_CUSTOM_EVT)
                    return;

                // First discovery custom event will be a snapshot operation.
                assertTrue(startedBySnapshot(fut));
                assertTrue("Snapshot must use pme-free exchange", fut.context().exchangeFreeSwitch());
            }

            /** {@inheritDoc} */
            @Override public void onInitAfterTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                if (fut.firstEvent().type() != EVT_DISCOVERY_CUSTOM_EVT)
                    return;

                DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)fut.firstEvent()).customMessage();

                assertNotNull(msg);

                if (msg instanceof SnapshotDiscoveryMessage)
                    loadLatch.countDown();
            }
        });

        // Start cache load
        IgniteInternalFuture<Long> loadFut = GridTestUtils.runMultiThreadedAsync(() -> {
            try {
                U.await(loadLatch);

                while (!Thread.currentThread().isInterrupted()) {
                    int txIdx = R.nextInt(grids);

                    // zero out the sign bit
                    grid(txIdx).cache(txCcfg.getName()).put(txKey.incrementAndGet(), R.nextInt() & Integer.MAX_VALUE);

                    int atomicIdx = R.nextInt(grids);

                    grid(atomicIdx).cache(DEFAULT_CACHE_NAME).put(atKey.incrementAndGet(), R.nextInt() & Integer.MAX_VALUE);
                }
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new RuntimeException(e);
            }
        }, 3, "cache-put-");

        try {
            IgniteFuture<Void> fut = ignite.snapshot().createSnapshot(snpName);

            U.await(loadLatch, 10, TimeUnit.SECONDS);

            fut.get();
        }
        finally {
            loadFut.cancel();
        }

        // cluster can be deactivated but we must test snapshot restore when binary recovery also occurred
        stopAllGrids();

        assertTrue("Snapshot directory must be empty for node not in baseline topology: " + notBltDirName,
            !searchDirectoryRecursively(locSnpDir.toPath(), notBltDirName).isPresent());

        IgniteEx snpIg0 = startGridsFromSnapshot(grids, snpName);

        assertEquals("The number of all (primary + backup) cache keys mismatch for cache: " + DEFAULT_CACHE_NAME,
            CACHE_KEYS_RANGE, snpIg0.cache(DEFAULT_CACHE_NAME).size());

        assertEquals("The number of all (primary + backup) cache keys mismatch for cache: " + txCcfg.getName(),
            CACHE_KEYS_RANGE, snpIg0.cache(txCcfg.getName()).size());

        snpIg0.cache(DEFAULT_CACHE_NAME).query(new ScanQuery<>(null))
            .forEach(e -> assertTrue("Snapshot must contains only negative values " +
                "[cache=" + DEFAULT_CACHE_NAME + ", entry=" + e +']', (Integer)e.getValue() < 0));

        snpIg0.cache(txCcfg.getName()).query(new ScanQuery<>(null))
            .forEach(e -> assertTrue("Snapshot must contains only negative values " +
                "[cache=" + txCcfg.getName() + ", entry=" + e + ']', (Integer)e.getValue() < 0));
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotPrimaryBackupsTheSame() throws Exception {
        int grids = 3;
        AtomicInteger cacheKey = new AtomicInteger();

        IgniteEx ignite = startGridsWithCache(grids, dfltCacheCfg, CACHE_KEYS_RANGE);

        IgniteInternalFuture<Long> atLoadFut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                int gId = R.nextInt(grids);

                grid(gId).cache(DEFAULT_CACHE_NAME)
                    .put(cacheKey.incrementAndGet(), 0);
            }
        }, 5, "atomic-cache-put-");

        IgniteInternalFuture<Long> txLoadFut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                int gId = R.nextInt(grids);

                IgniteCache<Integer, Integer> txCache = grid(gId).getOrCreateCache(txCcfg);

                try (Transaction tx = grid(gId).transactions().txStart()) {
                    txCache.put(cacheKey.incrementAndGet(), 0);

                    tx.commit();
                }
            }
        }, 5, "tx-cache-put-");

        try {
            IgniteFuture<Void> fut = ignite.snapshot().createSnapshot(SNAPSHOT_NAME);

            fut.get();
        }
        finally {
            txLoadFut.cancel();
            atLoadFut.cancel();
        }

        stopAllGrids();

        IgniteEx snpIg0 = startGridsFromSnapshot(grids, cfg -> snapshotPath(cfg).toString(), SNAPSHOT_NAME, false);

        // Block whole rebalancing.
        for (Ignite g : G.allGrids())
            TestRecordingCommunicationSpi.spi(g).blockMessages((node, msg) -> msg instanceof GridDhtPartitionDemandMessage);

        snpIg0.cluster().state(ACTIVE);

        assertFalse("Primary and backup in snapshot must have the same counters. Rebalance must not happen.",
            GridTestUtils.waitForCondition(() -> {
                boolean hasMsgs = false;

                for (Ignite g : G.allGrids())
                    hasMsgs |= TestRecordingCommunicationSpi.spi(g).hasBlockedMessages();

                return hasMsgs;
            }, REBALANCE_AWAIT_TIME));

        TestRecordingCommunicationSpi.stopBlockAll();
    }

    /** @throws Exception If fails. */
    @Test
    public void testRejectCacheStopDuringClusterSnapshot() throws Exception {
        // Block the full message, so cluster-wide snapshot operation would not be fully completed.
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg, CACHE_KEYS_RANGE);

        BlockingCustomMessageDiscoverySpi spi = discoSpi(ignite);
        spi.block((msg) -> {
            if (msg instanceof FullMessage) {
                FullMessage<?> msg0 = (FullMessage<?>)msg;

                assertEquals("Snapshot distributed process must be used",
                    DistributedProcess.DistributedProcessType.START_SNAPSHOT.ordinal(), msg0.type());

                assertTrue("Snapshot has to be finished successfully on all nodes", msg0.error().isEmpty());

                return true;
            }

            return false;
        });

        IgniteFuture<Void> fut = ignite.snapshot().createSnapshot(SNAPSHOT_NAME);

        spi.waitBlocked(10_000L);

        // Creating of new caches should not be blocked.
        ignite.getOrCreateCache(dfltCacheCfg.setName("default2"))
            .put(1, 1);

        forceCheckpoint();

        assertThrowsAnyCause(log,
            () -> {
                ignite.destroyCache(DEFAULT_CACHE_NAME);

                return 0;
            },
            IgniteCheckedException.class,
            SNP_IN_PROGRESS_ERR_MSG);

        spi.unblock();

        fut.get();
    }

    /** @throws Exception If fails. */
    @Test
    public void testBltChangeDuringClusterSnapshot() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg, CACHE_KEYS_RANGE);

        startGrid(3);

        long topVer = ignite.cluster().topologyVersion();

        BlockingCustomMessageDiscoverySpi spi = discoSpi(ignite);
        spi.block((msg) -> msg instanceof FullMessage);

        IgniteFuture<Void> fut = ignite.snapshot().createSnapshot(SNAPSHOT_NAME);

        spi.waitBlocked(10_000L);

        // Not baseline node joins successfully.
        String grid4Dir = folderName(startGrid(4));

        // Not blt node left the cluster and snapshot not affected.
        stopGrid(4);

        // Client node must connect successfully.
        startClientGrid(4);

        // Changing baseline complete successfully.
        ignite.cluster().setBaselineTopology(topVer);

        spi.unblock();

        fut.get();

        assertTrue("Snapshot directory must be empty for node 0 due to snapshot future fail: " + grid4Dir,
            !searchDirectoryRecursively(snp(ignite).snapshotLocalDir(SNAPSHOT_NAME).toPath(), grid4Dir).isPresent());
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotExOnInitiatorLeft() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        BlockingCustomMessageDiscoverySpi spi = discoSpi(ignite);
        spi.block((msg) -> msg instanceof FullMessage);

        IgniteFuture<Void> fut = ignite.snapshot().createSnapshot(SNAPSHOT_NAME);

        spi.waitBlocked(10_000L);

        ignite.close();

        assertThrowsAnyCause(log,
            fut::get,
            NodeStoppingException.class,
            SNP_NODE_STOPPING_ERR_MSG);
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotExistsException() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        assertThrowsAnyCause(log,
            () -> ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(),
            IgniteException.class,
            "Snapshot with given name already exists.");

        stopAllGrids();

        // Check that snapshot has not been accidentally deleted.
        IgniteEx snp = startGridsFromSnapshot(2, SNAPSHOT_NAME);

        assertSnapshotCacheKeys(snp.cache(dfltCacheCfg.getName()));
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCleanedOnLeft() throws Exception {
        CountDownLatch block = new CountDownLatch(1);
        CountDownLatch partProcessed = new CountDownLatch(1);

        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        File locSnpDir = snp(ignite).snapshotLocalDir(SNAPSHOT_NAME);
        String dirNameIgnite0 = folderName(ignite);

        IgniteSnapshotManager mgr1 = snp(grid(1));
        String dirNameIgnite1 = folderName(grid(1));

        Function<String, SnapshotSender> delegateLocSndr = (snpName) ->
            new DelegateSnapshotSender(log, mgr1.snapshotExecutorService(), mgr1.localSnapshotSender(snpName)) {
                @Override public void sendDelta0(File delta, String cacheDirName, GroupPartitionId pair) {
                    if (log.isInfoEnabled())
                        log.info("Processing delta file has been blocked: " + delta.getName());

                    partProcessed.countDown();

                    try {
                        U.await(block);

                        super.sendDelta0(delta, cacheDirName, pair);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        throw new IgniteException("Interrupted by node stop", e);
                    }
                }
            };

        GridTestUtils.setFieldValue(mgr1, "locSndrFactory", delegateLocSndr);

        TestRecordingCommunicationSpi commSpi1 = TestRecordingCommunicationSpi.spi(grid(1));
        commSpi1.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        IgniteFuture<?> fut = ignite.snapshot().createSnapshot(SNAPSHOT_NAME);

        U.await(partProcessed);

        stopGrid(1);

        block.countDown();

        assertThrowsAnyCause(log,
            fut::get,
            IgniteCheckedException.class,
            "Snapshot creation has been finished with an error");

        assertTrue("Snapshot directory must be empty for node 0 due to snapshot future fail: " + dirNameIgnite0,
            !searchDirectoryRecursively(locSnpDir.toPath(), dirNameIgnite0).isPresent());

        startGrid(1);

        awaitPartitionMapExchange();

        // Snapshot directory must be cleaned.
        assertTrue("Snapshot directory must be empty for node 1 due to snapshot future fail: " + dirNameIgnite1,
            !searchDirectoryRecursively(locSnpDir.toPath(), dirNameIgnite1).isPresent());

        List<String> allSnapshots = snp(ignite).getSnapshots();

        assertTrue("Snapshot directory must be empty due to snapshot fail: " + allSnapshots,
            allSnapshots.isEmpty());
    }

    /** @throws Exception If fails. */
    @Test
    public void testRecoveryClusterSnapshotJvmHalted() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        String grid0Dir = folderName(ignite);
        String grid1Dir = folderName(grid(1));
        File locSnpDir = snp(ignite).snapshotLocalDir(SNAPSHOT_NAME);

        jvm = true;

        IgniteConfiguration cfg2 = optimize(getConfiguration(getTestIgniteInstanceName(2)));

        cfg2.getDataStorageConfiguration()
            .setFileIOFactory(new HaltJvmFileIOFactory(new RandomAccessFileIOFactory(),
                (Predicate<File> & Serializable) file -> {
                    // Trying to create FileIO over partition file.
                    return file.getAbsolutePath().contains(SNAPSHOT_NAME);
                }));

        startGrid(cfg2);

        String grid2Dir = U.maskForFileName(cfg2.getConsistentId().toString());

        jvm = false;

        ignite.cluster().setBaselineTopology(ignite.cluster().topologyVersion());

        awaitPartitionMapExchange();

        assertThrowsAnyCause(log,
            () -> ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(),
            IgniteCheckedException.class,
            "Snapshot creation has been finished with an error");

        assertTrue("Snapshot directory must be empty: " + grid0Dir,
            !searchDirectoryRecursively(locSnpDir.toPath(), grid0Dir).isPresent());

        assertTrue("Snapshot directory must be empty: " + grid1Dir,
            !searchDirectoryRecursively(locSnpDir.toPath(), grid1Dir).isPresent());

        assertTrue("Snapshot directory must exist due to grid2 has been halted and cleanup not fully performed: " + grid2Dir,
            searchDirectoryRecursively(locSnpDir.toPath(), grid2Dir).isPresent());

        IgniteEx grid2 = startGrid(2);

        assertTrue("Snapshot directory must be empty after recovery: " + grid2Dir,
            !searchDirectoryRecursively(locSnpDir.toPath(), grid2Dir).isPresent());

        awaitPartitionMapExchange();

        assertTrue("Snapshot directory must be empty", grid2.snapshot().getSnapshots().isEmpty());

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME)
            .get();

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(2, SNAPSHOT_NAME);

        assertSnapshotCacheKeys(snp.cache(dfltCacheCfg.getName()));
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotWithRebalancing() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        TestRecordingCommunicationSpi commSpi = TestRecordingCommunicationSpi.spi(ignite);
        commSpi.blockMessages((node, msg) -> msg instanceof GridDhtPartitionSupplyMessage);

        startGrid(2);

        ignite.cluster().setBaselineTopology(ignite.cluster().topologyVersion());

        commSpi.waitForBlocked();

        IgniteFuture<Void> fut = ignite.snapshot().createSnapshot(SNAPSHOT_NAME);

        commSpi.stopBlock(true);

        fut.get();

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(3, SNAPSHOT_NAME);

        awaitPartitionMapExchange();

        assertSnapshotCacheKeys(snp.cache(dfltCacheCfg.getName()));
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotWithExplicitPath() throws Exception {
        File exSnpDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "ex_snapshots", true);

        try {
            IgniteEx ignite = null;

            for (int i = 0; i < 2; i++) {
                IgniteConfiguration cfg = optimize(getConfiguration(getTestIgniteInstanceName(i)));

                cfg.setSnapshotPath(exSnpDir.getAbsolutePath());

                ignite = startGrid(cfg);
            }

            ignite.cluster().baselineAutoAdjustEnabled(false);
            ignite.cluster().state(ACTIVE);

            for (int i = 0; i < CACHE_KEYS_RANGE; i++)
                ignite.cache(DEFAULT_CACHE_NAME).put(i, i);

            forceCheckpoint();

            ignite.snapshot().createSnapshot(SNAPSHOT_NAME)
                .get();

            stopAllGrids();

            IgniteEx snp = startGridsFromSnapshot(2, cfg -> exSnpDir.getAbsolutePath(), SNAPSHOT_NAME, true);

            assertSnapshotCacheKeys(snp.cache(dfltCacheCfg.getName()));
        }
        finally {
            stopAllGrids();

            U.delete(exSnpDir);
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return jvm;
    }

    /**
     * I/O Factory which will halt JVM on conditions occurred.
     */
    private static class HaltJvmFileIOFactory implements FileIOFactory {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /** Delegate factory. */
        private final FileIOFactory delegate;

        /** Condition to halt. */
        private final Predicate<File> pred;

        /**
         * @param delegate Delegate factory.
         */
        public HaltJvmFileIOFactory(FileIOFactory delegate, Predicate<File> pred) {
            this.delegate = delegate;
            this.pred = pred;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = this.delegate.create(file, modes);

            if (pred.test(file))
                Runtime.getRuntime().halt(Ignition.KILL_EXIT_CODE);

            return delegate;
        }
    }
}