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

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class IgniteBaselineRestoreMemoryBeforePMETest extends GridCommonAbstractTest {
    /** */
    private static final int GRIDS_COUNT = 3;

    /** Atomic cache name. */
    private static final String PARTITIONED_ATOMIC_CACHE_NAME = "p-atomic-cache";

    /** Entries. */
    private static final int ENTRIES = 3_000;

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

        waitForRebalancing();

        stopGrid(0);

        awaitPartitionMapExchange();

        System.out.println("Baseline topology nodes:");
        ignite(1)
            .cluster()
            .currentBaselineTopology()
            .stream()
            .map(BaselineNode::consistentId)
            .forEach(System.out::println);

        startGrid(0);

        awaitPartitionMapExchange();
    }
}
