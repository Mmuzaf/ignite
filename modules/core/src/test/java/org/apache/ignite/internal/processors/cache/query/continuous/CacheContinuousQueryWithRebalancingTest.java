package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

/**
 *
 */
public class CacheContinuousQueryWithRebalancingTest extends GridCommonAbstractTest {
    /** The delay between each 'put'. */
    private static final long DELAY = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(1)
                .setCacheMode(CacheMode.PARTITIONED)
                .setName(DEFAULT_CACHE_NAME));
    }

    /** @throws Exception If fails. */
    @Test
    public void testNodeStoppedOrdered() throws Exception {
        AtomicInteger counter = new AtomicInteger(0);

        int limit = 10;

        startWithQuery(1, counter);
        startWithQuery(2, counter);

        check(limit);

        assertEquals(limit, counter.get());
    }

    /** @throws Exception If fails. */
    @Test
    public void testNodeStoppedDisordered() throws Exception {
        AtomicInteger counter = new AtomicInteger(0);

        int limit = 10;

        startWithQuery(2, counter);
        startWithQuery(1, counter);

        check(limit);

        assertEquals(limit, counter.get());
    }

    /**
     * @param limit Events limit.
     * @throws Exception If fails.
     */
    private void check(int limit) throws Exception {
        IgniteEx ignite = grid(1);

        Integer key;

        // Wait for a key to be made available on the remote node.
        while ((key = keyForRemoteNode(ignite)) == null)
            doSleep(100);

        for (int i = 0; i < limit; i++) {

            try (Transaction transaction = ignite.transactions().txStart()) {
                log.warning("Writing <" + i + ">");

                ignite.cache(DEFAULT_CACHE_NAME).put(key, i);

                transaction.commit();
            }

            if (i == 3) {
                grid(2).close();
            }

            Thread.sleep(DELAY);
        }
    }

    /**
     * @param ignite Ignite instance.
     * @return Key.
     */
    private static Integer keyForRemoteNode(Ignite ignite) {
        Affinity<Integer> affinity = ignite.affinity(DEFAULT_CACHE_NAME);
        ClusterNode node = ignite.cluster().forRemotes().node();

        for (Integer i = 0; i < 100; i++) {
            if (affinity.mapKeyToNode(i).equals(node))
                return i;
        }

        return null;
    }

    /**
     * @param idx Ignite instance index.
     * @param counter Shared counter.
     * @throws Exception If fails.
     */
    private void startWithQuery(int idx, AtomicInteger counter) throws Exception {
        IgniteEx ignite = startGrid(idx);

        ContinuousQuery<String, Integer> query = new ContinuousQuery<>();
        query.setLocal(true);
        query.setLocalListener(entries -> entries.forEach(entry -> {
            counter.incrementAndGet();

            log.warning("Reading <" + entry.getValue() + "> on Node <" + ignite.localNode().id() + ">");
        }));

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);
        cache.query(query);
    }
}
