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

package org.apache.ignite.internal.util.nio.channel;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionKey;

/**
 *
 */
public class IgniteNioSocketChannelImpl implements IgniteNioSocketChannel {
    /** */
    private final ConnectionKey key;

    /** */
    private final SocketChannel channel;

    /** */
    private final IgniteNioSocketChannelConfig config;

    /** */
    private final AtomicBoolean readyStatus = new AtomicBoolean();

    /**
     * Create a new NIO socket channel.
     *
     * @param key Connection key.
     * @param channel The {@link SocketChannel} which will be used.
     */
    public IgniteNioSocketChannelImpl(ConnectionKey key, SocketChannel channel) {
        this.key = key;
        this.channel = channel;
        this.config = new IgniteNioSocketChannelConfig(channel);
    }

    /** {@inheritDoc} */
    @Override public ConnectionKey id() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public SocketChannel channel() {
        return channel;
    }

    /** {@inheritDoc} */
    @Override public IgniteNioSocketChannelConfig config() {
        return config;
    }

    /** {@inheritDoc} */
    @Override public boolean isReady() {
        return readyStatus.get();
    }

    /** {@inheritDoc} */
    @Override public void setReady() {
        boolean res = readyStatus.compareAndSet(false, true);

        assert res;
    }

    /** {@inheritDoc} */
    @Override public boolean isInputShutdown() {
        return channel().socket().isInputShutdown();
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> shutdownInput() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isOutputShutdown() {
        return channel().socket().isOutputShutdown();
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> shutdownOutput() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> closeFuture() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        U.closeQuiet(channel());
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        IgniteNioSocketChannelImpl channel1 = (IgniteNioSocketChannelImpl)o;

        if (!key.equals(channel1.key))
            return false;
        return channel.equals(channel1.channel);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + channel.hashCode();
        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "IgniteNioSocketChannelImpl{" +
            "key=" + key +
            ", channel=" + channel +
            ", config=" + config +
            '}';
    }
}
