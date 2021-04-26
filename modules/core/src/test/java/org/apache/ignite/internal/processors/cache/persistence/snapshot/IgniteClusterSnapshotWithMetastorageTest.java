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

import org.apache.ignite.internal.IgniteEx;
import org.junit.Before;
import org.junit.Test;

/**
 * Cluster-wide snapshot with distributed metastorage test.
 */
public class IgniteClusterSnapshotWithMetastorageTest extends AbstractSnapshotSelfTest {
    /** Cleanup data of task execution results if need. */
    @Before
    public void beforeCheck() throws Exception {
        cleanPersistenceDir();
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotWithMetastorage() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg, CACHE_KEYS_RANGE);
        startClientGrid();

        ignite.context().distributedMetastorage().write("key", "value");

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME)
            .get();

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(3, SNAPSHOT_NAME);

        assertEquals("value", snp.context().distributedMetastorage().read("key"));
    }
}
