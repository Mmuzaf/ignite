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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test cases for checking cancellation rebalancing process if some events occurs.
 */
public class GridCacheRebalancingCancelSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String REPLICATED_CACHE_PREFIX = "cacheR";

    /**
     * @throws Exception Exception to be thrown.
     */
    public void testAsyngCacheRebalancingStart() throws Exception {
        final IgniteEx ig0 = startGrid(0);

        for (int i = 0; i < 2; i++) {
            IgniteCache<Integer, Integer> cache = ig0.createCache(
                new CacheConfiguration<Integer, Integer>(REPLICATED_CACHE_PREFIX + i)
                    .setCacheMode(CacheMode.REPLICATED)
                    .setRebalanceMode(CacheRebalanceMode.ASYNC)
                    .setRebalanceOrder(i));

            for (int j = 0; j < 2048; j++)
                cache.put(j, j);
        }

        final IgniteEx ig1 = startGrid(1);

        final GridDhtPartitionDemander.RebalanceFuture rebFut0 =
            (GridDhtPartitionDemander.RebalanceFuture)ig1.context()
                .cache()
                .internalCache(REPLICATED_CACHE_PREFIX + 0)
                .preloader()
                .rebalanceFuture();

        final GridDhtPartitionDemander.RebalanceFuture rebFut1 =
            (GridDhtPartitionDemander.RebalanceFuture)ig1.context()
                .cache()
                .internalCache(REPLICATED_CACHE_PREFIX + 1)
                .preloader()
                .rebalanceFuture();

        // First rebalance future cancelled e.g. due to network exception.
        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                // result() return {@code false} means future cancelled.
                return rebFut0.isDone() && !rebFut0.result();
            }
        }, 10_000));

        // Second cache rebalance should be successfull, but NOT!
        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return rebFut1.isDone() && rebFut1.result();
            }
        }, 10_000));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new DelayableTcpCommunicationSpi());

        return cfg;
    }

    /**
     * Spi emulates network exception at the moment of initial demanded message sent for the first cache group.
     */
    private static class DelayableTcpCommunicationSpi extends TcpCommunicationSpi {
        /** */
        @Override public void sendMessage(ClusterNode node, Message msg,
            IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (msg instanceof GridIoMessage &&
                ((GridIoMessage)msg).message() instanceof GridDhtPartitionDemandMessage) {

                int grpId = ((GridCacheGroupIdMessage)((GridIoMessage)msg).message()).groupId();

                if (grpId == CU.cacheId(REPLICATED_CACHE_PREFIX + 0)) {
                    throw new IgniteSpiException("Attention! Failed to send message to remote node [" +
                        "node=" + node.id() +
                        ", msg=" + msg +
                        ", grp=" + (REPLICATED_CACHE_PREFIX + 0) + ']');
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
