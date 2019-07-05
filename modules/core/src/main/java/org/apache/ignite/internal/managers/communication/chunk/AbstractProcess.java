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

package org.apache.ignite.internal.managers.communication.chunk;

import java.io.Closeable;
import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Class represents base object which can we transferred (written or readed) by chunks of
 * predefined size over a socket channel.
 */
abstract class AbstractProcess implements Closeable {
    /** Additional stream params. */
    @GridToStringInclude
    private final Map<String, Serializable> params = new HashMap<>();

    /** The size of segment for the read. */
    private int chunkSize;

    /** The unique input name to identify particular transfer part. */
    private String name;

    /**
     * The position from which the transfer will start. For the {@link File} it will be offset
     * where the transfer begin data transfer.
     */
    private long startPos;

    /** The total number of bytes to send. */
    private long cnt;

    /** Node stopping checker. */
    private Supplier<Boolean> stopChecker;

    /** The number of bytes successfully transferred druring iteration. */
    protected long transferred;

    /**
     * @param name The unique file name within transfer process.
     * @param startPos The position from which the transfer should start to.
     * @param cnt The number of bytes to expect of transfer.
     * @param params Additional stream params.
     * @param stopChecker Node stop or prcoess interrupt checker.
     */
    protected AbstractProcess(
        String name,
        long startPos,
        long cnt,
        Map<String, Serializable> params,
        Supplier<Boolean> stopChecker
    ) {
        this.name = name;
        this.startPos = startPos;
        this.cnt = cnt;
        this.stopChecker = stopChecker;

        if (params != null)
            this.params.putAll(params);
    }

    /**
     * @return Name of chunked object.
     */
    public String name() {
        return name;
    }

    /**
     * @return Start chunked object position (same as a file offset) .
     */
    public long startPosition() {
        return startPos;
    }

    /**
     * @return Number of bytes to transfer (read from or write to channel).
     */
    public long count() {
        return cnt;
    }

    /**
     * @return Size of each chunk in bytes.
     */
    public int chunkSize() {
        return chunkSize;
    }

    /**
     * @return Number of bytes which has been transfered.
     */
    public long transferred() {
        return transferred;
    }

    /**
     * @return Additional chunked object params.
     */
    public Map<String, Serializable> params() {
        return params;
    }

    /**
     * @return {@code true} if the process of data sending\receiving must be interrupt.
     */
    public boolean stopped() {
        return stopChecker.get();
    }

    /**
     * @param chunkSize The size of chunk in bytes.
     */
    protected void chunkSize(int chunkSize) {
        assert chunkSize > 0;

        this.chunkSize = chunkSize;
    }

    /**
     * @param cnt The number of bytes which has been already transferred.
     */
    protected void transferred(long cnt) {
        assert cnt >= 0;

        transferred = cnt;
    }

    /**
     * @return {@code true} if and only if a chunked object has received all the data it expects.
     */
    protected boolean hasNextChunk() {
        return transferred < cnt;
    }

    /**
     * @throws IgniteCheckedException If fails.
     */
    protected void checkTransferLimitCount() throws IgniteCheckedException {
        if (transferred > cnt) {
            throw new IgniteCheckedException("File has been transferred with incorrect size " +
                "[expect=" + cnt + ", actual=" + transferred + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AbstractProcess.class, this);
    }
}
