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

package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;

/**
 *
 */
public class IgniteClientReconnectApiExceptionAtomicLongTest extends IgniteClientReconnectAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 1;
    }

    /**
     * Get {@link CollectionConfiguration} with number of backups equal to {@link AtomicConfiguration} default
     *
     */
    private CollectionConfiguration getCollectionConfiguration() {
        return new CollectionConfiguration().setBackups(0);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testErrorOnDisconnect() throws Exception {
        clientMode = true;

        final Ignite client = startGrid(serverCount());

        final CountDownLatch operationOrder = new CountDownLatch(1);

        final List<T2<Callable, C1<Object, Boolean>>> ops =
            Arrays.asList(
                // Check atomic long.
                new T2<Callable, C1<Object, Boolean>>(
                    new Callable() {
                        @Override public Object call() throws Exception {
                            boolean failed = false;

                            try {
                                client.atomicLong("testAtomic", 41, true);
                            }
                            catch (IgniteClientDisconnectedException e) {
                                failed = true;

                                checkAndWait(e);
                            }

                            assertTrue(failed);

                            info("AtomicLong call");
                            IgniteAtomicLong igniteAtomicLong =
                                client.atomicLong("testAtomic", 41, true);

                            operationOrder.countDown();

                            return igniteAtomicLong;
                        }
                    },
                    new C1<Object, Boolean>() {
                        @Override public Boolean apply(Object o) {
                            assertNotNull(o);

                            IgniteAtomicLong atomicLong = (IgniteAtomicLong)o;

                            assertEquals(42, atomicLong.incrementAndGet());

                            return true;
                        }
                    }
                ),
                // Check ignite queue.
                new T2<Callable, C1<Object, Boolean>>(
                    new Callable() {
                        @Override public Object call() throws Exception {
                            boolean failed = false;

                            try {
                                client.queue("TestQueue", 10, getCollectionConfiguration());
                            }
                            catch (IgniteClientDisconnectedException e) {
                                failed = true;

                                checkAndWait(e);
                            }

                            assertTrue(failed);

                            operationOrder.await(20, SECONDS);

                            info("TestQueue call");
                            return client.queue("TestQueue", 10, getCollectionConfiguration());
                        }
                    },
                    new C1<Object, Boolean>() {
                        @Override public Boolean apply(Object o) {
                            assertNotNull(o);

                            IgniteQueue queue = (IgniteQueue)o;

                            String val = "Test";

                            queue.add(val);

                            assertEquals(val, queue.poll());

                            return true;
                        }
                    }
                )
            );

        clientMode = false;

        final TestTcpDiscoverySpi clientSpi = spi(client);

        Ignite srv = clientRouter(client);

        TestTcpDiscoverySpi srvSpi = spi(srv);

        final CountDownLatch disconnectLatch = new CountDownLatch(1);

        final CountDownLatch reconnectLatch = new CountDownLatch(1);

        log.info("Block reconnect.");

        clientSpi.writeLatch = new CountDownLatch(1);

        final List<IgniteInternalFuture> futs = new ArrayList<>();

        client.events().localListen(
            new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                        info("Disconnected: " + evt);

                        assertEquals(1, reconnectLatch.getCount());

                        for (T2<Callable, C1<Object, Boolean>> op : ops) {
                            futs.add(GridTestUtils.runAsync(op.get1()));
                        }
                        disconnectLatch.countDown();
                    }
                    else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                        info("Reconnected: " + evt);

                        reconnectLatch.countDown();
                    }

                    return true;
                }
            },
            EVT_CLIENT_NODE_DISCONNECTED,
            EVT_CLIENT_NODE_RECONNECTED
        );

        try {
            log.info("Fail client.");

            srvSpi.failNode(client.cluster().localNode().id(), null);

            waitReconnectEvent(disconnectLatch);

            assertEquals(ops.size(), futs.size());

            for (IgniteInternalFuture<?> fut : futs)
                assertNotDone(fut);

            U.sleep(2000);

            for (IgniteInternalFuture<?> fut : futs)
                assertNotDone(fut);

            log.info("Allow reconnect.");

            clientSpi.writeLatch.countDown();

            waitReconnectEvent(reconnectLatch);

            // Check operation after reconnect working.
            for (int i = 0; i < futs.size(); i++) {
                try {
                    final Object futRes = futs.get(i).get(20, SECONDS);

                    assertTrue(ops.get(i).get2().apply(futRes));
                }
                catch (IgniteFutureTimeoutCheckedException e) {
                    e.printStackTrace();

                    fail("Operation timeout. Iteration: " + i + ".");
                }
            }
        }
        finally {
            clientSpi.writeLatch.countDown();

            for (IgniteInternalFuture fut : futs)
                fut.cancel();

            stopAllGrids();
        }
    }

}
