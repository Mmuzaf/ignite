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

package org.apache.ignite.internal.processors.cache.persistence.backup;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
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
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointFuture;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;

/** */
public class IgniteBackupManagerSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int CACHE_PARTS_COUNT = 8;

    /** */
    private static final int PAGE_SIZE = 1024;

    /** */
    private static final DataStorageConfiguration memCfg = new DataStorageConfiguration()
        .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(100L * 1024 * 1024)
            .setPersistenceEnabled(true))
        .setPageSize(PAGE_SIZE)
        .setWalMode(WALMode.LOG_ONLY);

    /** */
    private CacheConfiguration<Integer, Integer> defaultCacheCfg =
        new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false)
                .setPartitions(CACHE_PARTS_COUNT));

    /** Directory to store temporary files on testing cache backup process. */
    private File backupDir;

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

    /**
     * @param from File to copy from.
     * @param offset Starting file position.
     * @param count Bytes to copy to destination.
     * @param to Output directory.
     * @throws IgniteCheckedException If fails.
     */
    private static File copy(File from, long offset, long count, File to) throws IgniteCheckedException {
        assert to.isDirectory();

        try {
            File destFile = new File(to, from.getName());

            if (!destFile.exists() || destFile.delete())
                destFile.createNewFile();

            try (FileChannel src = new FileInputStream(from).getChannel();
                 FileChannel dest = new FileOutputStream(destFile).getChannel()) {
                src.position(offset);

                long written = 0;

                while (written < count)
                    written += src.transferTo(written, count - written, dest);
            }

            return destFile;
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** */
    @Before
    public void beforeTestBackup() throws Exception {
        cleanPersistenceDir();

        backupDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "backup", true);
    }

    /** */
    @After
    public void afterTestBackup() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setDataStorageConfiguration(memCfg)
            .setCacheConfiguration(defaultCacheCfg);
    }

    /**
     *
     */
    @Test
    public void testBackupLocalPartitions() throws Exception {
        // Start grid node with data before each test.
        IgniteEx ig = startGridWithCache(defaultCacheCfg);

        for (int i = 1024; i < 2048; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, i);

        Map<Integer, Set<Integer>> toBackup = new HashMap<>();

        toBackup.put(CU.cacheId(DEFAULT_CACHE_NAME),
            Stream.iterate(0, n -> n + 1)
            .limit(CACHE_PARTS_COUNT)
            .collect(Collectors.toSet()));

        IgniteInternalFuture<?> backupFut = ig.context()
            .cache()
            .context()
            .backup()
            .createLocalBackup("testBackup", toBackup, backupDir);

        backupFut.get();

        File cacheWorkDir = ((FilePageStoreManager)ig.context()
            .cache()
            .context()
            .pageStore())
            .cacheWorkDir(defaultCacheCfg);

        // Calculate CRCs
        final Map<String, Integer> origParts = calculateCRC32Partitions(cacheWorkDir);

        final Map<String, Integer> bakcupCRCs = calculateCRC32Partitions(new File(new File(backupDir.getAbsolutePath(),
            "testBackup"),
            cacheDirName(defaultCacheCfg)));

        assertEquals("Partitons the same after backup and after merge", origParts, bakcupCRCs);
    }

    /**
     *
     */
    @Test
    public void testBackupLocalPartitionsNextCpStarted() throws Exception {
        CountDownLatch slowCopy = new CountDownLatch(1);
        AtomicBoolean stopper = new AtomicBoolean();

        IgniteEx ig = startGridWithCache(defaultCacheCfg.setAffinity(new ZeroPartitionAffinityFunction()
            .setPartitions(CACHE_PARTS_COUNT)));

        AtomicLong key = new AtomicLong();
        AtomicLong value = new AtomicLong();

        GridTestUtils.runAsync(() -> {
                try {
                    while (!stopper.get() && !Thread.currentThread().isInterrupted()) {
                        ig.cache(DEFAULT_CACHE_NAME)
                            .put(key.incrementAndGet(), value.incrementAndGet());

                        U.sleep(10);
                    }
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw new IgniteException(e);
                }
            });

        Map<Integer, Set<Integer>> toBackup = new HashMap<>();

        toBackup.put(CU.cacheId(DEFAULT_CACHE_NAME),
            Stream.iterate(0, n -> n + 1)
                .limit(CACHE_PARTS_COUNT)
                .collect(Collectors.toSet()));

        File cacheWorkDir = ((FilePageStoreManager)ig.context()
            .cache()
            .context()
            .pageStore())
            .cacheWorkDir(defaultCacheCfg);

        File zeroPart = getPartitionFile(cacheWorkDir, 0);

        IgniteBackupManager mgr = ig.context()
            .cache()
            .context()
            .backup();

        IgniteInternalFuture<?> backupFut = mgr
            .createLocalBackup("testBackup",
                toBackup,
                backupDir,
                new IgniteTriClosure<File, File, Long, Supplier<File>>() {
                    @Override public Supplier<File> apply(File from, File to, Long length) {
                        return new Supplier<File>() {
                            @Override public File get() {
                                try {
                                    if (from.getName().trim().equals(zeroPart.getName()))
                                        U.await(slowCopy);

                                    return mgr.partSupplierFactory(from, to, length).get();
                                }
                                catch (IgniteInterruptedCheckedException e) {
                                    throw new IgniteException(e);
                                }
                            }
                        };
                    }
                },
                mgr::deltaSupplierFactory);

        // Backup on the next checkpoint must copy page before write it to partition
        CheckpointFuture cpFut = ig.context()
            .cache()
            .context()
            .database()
            .forceCheckpoint("second cp");

        cpFut.finishFuture().get();

        stopper.set(true);
        slowCopy.countDown();

        backupFut.get();
    }

    /** */
    private IgniteEx startGridWithCache(CacheConfiguration<Integer, Integer> ccfg) throws Exception {
        defaultCacheCfg = ccfg;

        // Start grid node with data before each test.
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        for (int i = 0; i < 1024; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, i);

        CheckpointFuture cpFut = ig.context()
            .cache()
            .context()
            .database()
            .forceCheckpoint("the next one");

        cpFut.finishFuture().get();

        return ig;
    }

    /** */
    private void partitionCRCs(PageStore pageStore, int partId) throws IgniteCheckedException {
        long pageId = PageIdUtils.pageId(partId, FLAG_DATA, 0);

        ByteBuffer buf = ByteBuffer.allocate(pageStore.getPageSize())
            .order(ByteOrder.nativeOrder());

        StringBuilder sb = new StringBuilder();

        for (int pageNo = 0; pageNo < pageStore.pages(); pageId++, pageNo++) {
            buf.clear();

            pageStore.read(pageId, buf, true);

            sb.append("[pageId=")
                .append(pageId)
                .append(", crc=")
                .append(PageIO.getCrc(buf))
                .append("]\n");
        }

        U.log(log, sb.append("[pages=").append(pageStore.pages()).append("]\n").toString());
    }

    /**
     *
     */
    private static class ZeroPartitionAffinityFunction extends RendezvousAffinityFunction {
        @Override public int partition(Object key) {
            return 0;
        }
    }
}
