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

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Buffered chunked receiver can handle input socket channel by chunks of data and
 * deliver it to an allocated {@link ByteBuffer}.
 */
class ChunkReceiver extends AbstractReceiver {
    /** Chunked channel handler to process data with chunks. */
    private final ChunkHandler hnd;

    /** The destination object to transfer data to\from. */
    private ByteBuffer buf;

    /**
     * @param nodeId The remote node id receive request for transmission from.
     * @param name The unique file name within transfer process.
     * @param startPos The position from which the transfer should start to.
     * @param cnt The number of bytes to expect of transfer.
     * @param params Additional stream params.
     * @param chunkSize Size of chunks.
     * @param stopChecker Node stop or prcoess interrupt checker.
     * @param hnd Transmission handler to process download result.
     * @throws IgniteCheckedException If fails.
     */
    public ChunkReceiver(
        UUID nodeId,
        String name,
        long startPos,
        long cnt,
        Map<String, Serializable> params,
        int chunkSize,
        Supplier<Boolean> stopChecker,
        TransmissionHandler hnd
    ) throws IgniteCheckedException {
        super(name, startPos, cnt, params, stopChecker);

        assert chunkSize > 0;

        this.hnd = hnd.chunkHandler(nodeId, name, startPos, cnt, params);

        assert this.hnd != null : "ChunkHandler must be provided by transmission handler";

        int buffSize = this.hnd.size();
        this.chunkSize = buffSize > 0 ? buffSize : chunkSize;;
    }

    /** {@inheritDoc} */
    @Override public void receive(
        ReadableByteChannel ch,
        TransmissionMeta meta
    ) throws IOException, IgniteCheckedException {
        try {
            super.receive(ch, meta);
        } catch (IgniteCheckedException e) {
            hnd.close();

            throw e;
        }

        // If an IOException occurs, reconnect will happen.
        if (transferred == total)
            hnd.close();
    }

    /** {@inheritDoc} */
    @Override protected TransmissionPolicy policy() {
        return TransmissionPolicy.CHUNK;
    }

    /** {@inheritDoc} */
    @Override protected void init(TransmissionMeta meta) throws IgniteCheckedException {
        assert meta != null;
        assert buf == null;

        buf = ByteBuffer.allocate(chunkSize);
        buf.order(ByteOrder.nativeOrder());
    }

    /** {@inheritDoc} */
    @Override protected void readChunk(ReadableByteChannel ch) throws IOException, IgniteCheckedException {
        assert buf != null : "Buffer is used to deilver readed data to the used and cannot be null: " + this;

        buf.rewind();

        int readed = 0;
        int res;

        // Read data from input channel utill the buffer will be completely filled
        // (buf.remaining() returns 0) or partitially filled buffer if it was the last chunk.
        while (true) {
            res = ch.read(buf);

            // Read will return -1 if remote node close connection.
            if (res < 0) {
                if (transferred + readed != total) {
                    throw new IOException("Input data channel reached its end, but file has not fully loaded " +
                        "[transferred=" + transferred + ", readed=" + readed + ", total=" + total + ']');
                }

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

        hnd.accept(buf);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        buf = null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChunkReceiver.class, this, "super", super.toString());
    }
}
