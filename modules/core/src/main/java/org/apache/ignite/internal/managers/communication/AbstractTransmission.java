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
import java.io.Serializable;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Class represents base object which can transmit files (read or write) by chunks of
 * predefined size over an opened {@link SocketChannel}.
 */
abstract class AbstractTransmission implements Closeable {
    /** Node stopping checker. */
    private final Supplier<Boolean> stopChecker;

    /** Ignite logger. */
    protected final IgniteLogger log;

    /** Initial meta with file transferred attributes. */
    protected final TransmissionMeta initMeta;

    /** The number of bytes successfully transferred druring iteration. */
    protected long transferred;

    /** The size of segment for the read. */
    protected int chunkSize;

    /**
     * @param initMeta Initial file meta info.
     * @param stopChecker Node stop or prcoess interrupt checker.
     * @param log Ignite logger.
     */
    protected AbstractTransmission(TransmissionMeta initMeta, Supplier<Boolean> stopChecker, IgniteLogger log) {
        A.notNull(initMeta, "Initial file meta cannot be null");
        A.notNullOrEmpty(initMeta.name(), "Trasmisson name cannot be empty or null");
        A.ensure(initMeta.offset() >= 0, "File start position cannot be negative");
        A.ensure(initMeta.count() > 0, "Total number of bytes to transfer must be greater than zero");
        A.notNull(stopChecker, "Process stop checker cannot be null");

        this.stopChecker = stopChecker;
        this.initMeta = initMeta;
        this.log = log.getLogger(AbstractTransmission.class);
    }

    /**
     * @return String representation file name.
     */
    public String name() {
        return initMeta.name();
    }

    /**
     * @return Transferred file position offset.
     */
    public long offset() {
        return initMeta.offset();
    }

    /**
     * @return Number of bytes to transfer (read from or write to channel).
     */
    public long count() {
        return initMeta.count();
    }

    /**
     * @return Map of additional file params.
     */
    public Map<String, Serializable> params() {
        return initMeta.params();
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
        return transferred < initMeta.count();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AbstractTransmission.class, this);
    }
}
