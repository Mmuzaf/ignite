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

package org.apache.ignite.internal.managers.communication;

import java.io.Closeable;
import java.io.File;
import java.io.Serializable;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Class represents base object which can transmit files (read or write) by chunks of
 * predefined size over an opened {@link SocketChannel}.
 */
abstract class AbstractTransmission implements Closeable {
    /** Node stopping checker. */
    private final Supplier<Boolean> stopChecker;

    /**
     * The position from which the transfer will start. For the {@link File} it will be offset
     * where the transfer begin data transfer.
     */
    protected final long startPos;

    /** The total number of bytes to send or receive. */
    protected final long total;

    /** The unique input name to identify particular transfer part. */
    protected final String name;

    /** Additional stream params. */
    @GridToStringInclude
    protected final Map<String, Serializable> params = new HashMap<>();

    /** The number of bytes successfully transferred druring iteration. */
    protected long transferred;

    /** The size of segment for the read. */
    protected int chunkSize;

    /**
     * @param name The unique file name within transfer process.
     * @param startPos The position from which the transfer should start to.
     * @param total The number of bytes to expect of transfer.
     * @param params Additional stream params.
     * @param stopChecker Node stop or prcoess interrupt checker.
     */
    protected AbstractTransmission(
        String name,
        long startPos,
        long total,
        Map<String, Serializable> params,
        Supplier<Boolean> stopChecker
    ) {
        A.notNullOrEmpty(name, "Trasmisson name cannot be empty or null");
        A.ensure(startPos >= 0, "File start position cannot be negative");
        A.ensure(total > 0, "Total number of bytes to transfer must be greater than zero");
        A.notNull(stopChecker, "Process stop checker cannot be null");

        this.name = name;
        this.startPos = startPos;
        this.total = total;
        this.stopChecker = stopChecker;

        if (params != null)
            this.params.putAll(params);
    }

    /**
     * @return Number of bytes to transfer (read from or write to channel).
     */
    public long total() {
        return total;
    }

    /**
     * @return Number of bytes which has been transferred.
     */
    public long transferred() {
        return transferred;
    }

    /**
     * @return {@code true} if the transmission process should be interrupted.
     */
    protected boolean stopped() {
        return stopChecker.get();
    }

    /**
     * @return {@code true} if and only if a chunked object has received all the data it expects.
     */
    protected boolean hasNextChunk() {
        return transferred < total;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AbstractTransmission.class, this);
    }
}
