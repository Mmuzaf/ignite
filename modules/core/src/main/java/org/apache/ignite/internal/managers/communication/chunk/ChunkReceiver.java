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

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.communication.ChunkHandler;
import org.apache.ignite.internal.managers.communication.ReadPolicy;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Buffered chunked receiver can handle input socket channel by chunks of data and
 * deliver it to an allocated {@link ByteBuffer}.
 */
public class ChunkReceiver extends AbstractReceiver {
    /** Chunked channel handler to process data with chunks. */
    private final ChunkHandler handler;

    /** The destination object to transfer data to\from. */
    private ByteBuffer buf;

    /**
     * @param name The unique file name within transfer process.
     * @param startPos The position from which the transfer should start to.
     * @param cnt The number of bytes to expect of transfer.
     * @param params Additional stream params.
     * @param stopChecker Node stop or prcoess interrupt checker.
     * @param handler The chunk handler to process each chunk.
     */
    public ChunkReceiver(
        String name,
        long startPos,
        long cnt,
        Map<String, Serializable> params,
        Supplier<Boolean> stopChecker,
        ChunkHandler handler
    ) {
        super(name, startPos, cnt, params, stopChecker);

        assert handler != null;

        this.handler = handler;
    }

    /** {@inheritDoc} */
    @Override protected ReadPolicy policy() {
        return ReadPolicy.BUFF;
    }

    /** {@inheritDoc} */
    @Override protected void init(int chunkSize) throws IgniteCheckedException {
        assert buf == null;

        int buffSize = handler.size();

        int size = buffSize > 0 ? buffSize : chunkSize;

        chunkSize(size);

        buf = ByteBuffer.allocate(size);
        buf.order(ByteOrder.nativeOrder());
    }

    /** {@inheritDoc} */
    @Override protected void readChunk(ReadableByteChannel ch) throws IOException, IgniteCheckedException {
        buf.rewind();

        int readed = 0;
        int res;

        // Read data from input channel utill the buffer will be completely filled
        // (buf.remaining() returns 0) or partitially filled buffer if it was the last chunk.
        while (true) {
            res = ch.read(buf);

            if (res < 0) {
                if (transferred + readed != total)
                    throw new IOException("Input data channel reached its end, but chunked object has not fully loaded");

                break;
            }

            readed += res;

            if (readed == buf.capacity() || buf.position() == buf.capacity())
                break;
        }

        if (readed == 0)
            return;

        transferred += readed;

        buf.flip();

        handler.accept(buf);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        buf = null;

        handler.close();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChunkReceiver.class, this, "super", super.toString());
    }
}
