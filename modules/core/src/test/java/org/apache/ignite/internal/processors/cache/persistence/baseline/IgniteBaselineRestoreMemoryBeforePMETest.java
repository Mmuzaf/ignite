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
package org.apache.ignite.internal.processors.cache.persistence.baseline;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.StorageException;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/** */
public class IgniteBaselineRestoreMemoryBeforePMETest extends GridCommonAbstractTest {
    /** */
    private static final int GRIDS_COUNT = 3;

    /** Atomic cache name. */
    private static final String PARTITIONED_ATOMIC_CACHE_NAME = "p-atomic-cache";

    /** Entries. */
    private static final int ENTRIES = 3_000;

    /** */
    private static boolean slow = false;

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setInitialSize(200L * 1024 * 1024)
                    .setMaxSize(200L * 1024 * 1024)
                    .setCheckpointPageBufferSize(200L * 1024 * 1024)
                )
        );

        CacheConfiguration<Integer, String> cCfg =
            new CacheConfiguration<Integer, String>(PARTITIONED_ATOMIC_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setBackups(2);

        cfg.setCacheConfiguration(cCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        startGrids(GRIDS_COUNT);

        grid(0).cluster().active(true);

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception if fails.
     */
    public void testBaselineClusterNodeLeftJoin() throws Exception {
        for (int i = 0; i < ENTRIES; i++) {
            ThreadLocalRandom r = ThreadLocalRandom.current();

            byte[] randBytes = new byte[r.nextInt(10, 100)];

            ignite(0)
                .getOrCreateCache(PARTITIONED_ATOMIC_CACHE_NAME)
                .put(r.nextInt(ENTRIES), new String(randBytes));
        }

        awaitPartitionMapExchange();

        stopGrid(0);

        awaitPartitionMapExchange();

        System.out.println("Baseline topology nodes:");
        ignite(1)
            .cluster()
            .currentBaselineTopology()
            .stream()
            .map(BaselineNode::consistentId)
            .forEach(System.out::println);

        log.info("Start slowing FWAL");

        slow = true;

        IgniteEx ig0 = startGrid(0);

        slow = false;

        IgniteWriteAheadLogManager wal = ig0.context().cache().context().wal();

        awaitPartitionMapExchange();
    }

    /** */
    public static class FileWriteAheadLogManagerProvider implements PluginProvider, IgnitePlugin, IgniteChangeGlobalStateSupport {
        /** */
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public String name() {
            return "Wal Plugin";
        }

        /** {@inheritDoc} */
        @Override public String version() {
            return "1.0";
        }

        /** {@inheritDoc} */
        @Override public String copyright() {
            return "Apache Ignite (c)";
        }

        /** {@inheritDoc} */
        @Override public IgnitePlugin plugin() {
            return this;
        }

        /** {@inheritDoc} */
        @Override public void initExtensions(PluginContext ctx,
            ExtensionRegistry registry) throws IgniteCheckedException {
            log = ctx.log(getClass());
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object createComponent(PluginContext ctx, Class cls) {
            if (IgniteWriteAheadLogManager.class.equals(cls)) {
                assert ctx.igniteConfiguration().getDataStorageConfiguration().getWalMode() != WALMode.FSYNC;

                return new FileWriteAheadLogManager(((IgniteEx)ctx.grid()).context()) {
                    /** {@inheritDoc} */
                    @Override public WALIterator replay(
                        WALPointer start) throws IgniteCheckedException, StorageException {
                        FileWriteAheadLogManagerProvider.this.log.info("FileWriteAheadLogManager Plugin");

                        if (slow)
                            U.sleep(10_000);

                        return super.replay(start);
                    }
                };
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void start(PluginContext ctx) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void stop(boolean cancel) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onIgniteStart() throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onIgniteStop(boolean cancel) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Nullable @Override public Serializable provideDiscoveryData(UUID nodeId) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onDeActivate(GridKernalContext kctx) {
            //No-op.
        }
    }


}
