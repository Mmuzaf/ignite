/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.preload;

import java.util.Collections;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
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
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingSyncSelfTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_DISABLED;

/**
 * Test cases for checking cancellation rebalancing process if some events occurs.
 */
public class GridCachePersistenceRebalanceSelfTest extends GridCacheRebalancingSyncSelfTest {
    /** */
    @Before
    public void setBefore() throws Exception {
        cleanPersistenceDir();

        // Turn off for the debug
        System.setProperty(IGNITE_JVM_PAUSE_DETECTOR_DISABLED, "true");
        System.setProperty(IGNITE_DUMP_THREADS_ON_FAILURE, "false");

        // Tests
        System.setProperty(IgniteSystemProperties.IGNITE_PERSISTENCE_REBALANCE_ENABLED, "true");
        System.setProperty(IGNITE_BASELINE_AUTO_ADJUST_ENABLED, "false");
    }

    /** */
    @After
    public void setAfter() {
        System.setProperty(IgniteSystemProperties.IGNITE_PERSISTENCE_REBALANCE_ENABLED, "false");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                        .setMaxSize(100L * 1024 * 1024)
                        .setPersistenceEnabled(true))
                    .setWalMode(WALMode.LOG_ONLY))
            .setCacheConfiguration(
                new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setRebalanceMode(CacheRebalanceMode.ASYNC)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                    .setBackups(1)
                    .setAffinity(new RendezvousAffinityFunction(false)
                        .setPartitions(8)));
    }

    /** */
    @Test
    public void testPersistenceRebalanceBase() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ignite0.getOrCreateCache(DEFAULT_CACHE_NAME);

        generateData(ignite0, DEFAULT_CACHE_NAME, 0, 0);

        assertTrue(!ignite0.cluster().baselineConfiguration().isBaselineAutoAdjustEnabled());

        IgniteEx ignite1 = startGrid(1);

        ignite1.cluster().setBaselineTopology(ignite1.cluster().nodes());

        awaitPartitionMapExchange(true, true, Collections.singleton(ignite1.localNode()), true);
    }
}
