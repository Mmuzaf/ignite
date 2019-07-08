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
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.communication.TransmissionMeta;
import org.apache.ignite.internal.managers.communication.TransmissionPolicy;

import static org.apache.ignite.internal.util.IgniteUtils.assertParameter;

/**
 * Class represents a receiver of data which can be pulled from a channel by chunks of
 * predefined size. Closes when a transmission of represented object ends.
 */
public abstract class AbstractReceiver extends AbstractTransmission {
    /**
     * @param name The unique file name within transfer process.
     * @param startPos The position from which the transfer should start to.
     * @param cnt The number of bytes to expect of transfer.
     * @param params Additional stream params.
     * @param stopChecker Node stop or prcoess interrupt checker.
     */
    protected AbstractReceiver(
        String name,
        long startPos,
        long cnt,
        Map<String, Serializable> params,
        Supplier<Boolean> stopChecker
    ) {
        super(name, startPos, cnt, params, stopChecker);
    }

    /**
     * @return File name processing by receiver.
     */
    public String name() {
        return name;
    }

    /**
     * @param ch Input channel to read data from.
     * @param meta Meta information about receiving file.
     * @param chunkSize Size of chunks for receiver.
     * @throws IOException If an io exception occurred.
     * @throws IgniteCheckedException If some check failed.
     */
    public void receive(
        ReadableByteChannel ch,
        TransmissionMeta meta,
        int chunkSize
    ) throws IOException, IgniteCheckedException {
        assert meta != null;
        assert chunkSize > 0;

        assertParameter(name.equals(meta.name()), "Attempt to load different file " +
            "[name=" + name + ", meta=" + meta + ']');

        assertParameter(startPos + transferred == meta.offset(),
            "The next chunk offest is incorrect [startPos=" + startPos +
                ", transferred=" + transferred + ", meta=" + meta + ']');

        assertParameter(total == meta.count() + transferred, " The count of bytes to transfer for " +
            "the next chunk is incorrect [total=" + total + ", transferred=" + transferred +
            ", startPos=" + startPos + ", meta=" + meta + ']');

        init(chunkSize, meta);

        // Read data from the input.
        while (hasNextChunk()) {
            if (Thread.currentThread().isInterrupted() || stopped()) {
                throw new IgniteCheckedException("Thread has been interrupted or operation has been cancelled " +
                    "due to node is stopping. Channel processing has been stopped.");
            }

            readChunk(ch);
        }

        assertTransferredBytes();
    }

    /**
     * @return Current receiver state written to a {@link TransmissionMeta} instance.
     */
    public TransmissionMeta state() {
        return new TransmissionMeta(name,
            startPos + transferred,
            total,
            transferred == 0,
            params,
            policy(),
            null,
            null);
    }

    /**
     * @return Read policy of data handling.
     */
    protected abstract TransmissionPolicy policy();

    /**
     * @param chunkSize Size of chunks.
     * @param meta Meta information about receiving file.
     * @throws IgniteCheckedException If fails.
     */
    protected abstract void init(int chunkSize, TransmissionMeta meta) throws IgniteCheckedException;

    /**
     * @param ch Channel to read data from.
     * @throws IOException If fails.
     * @throws IgniteCheckedException If fails.
     */
    protected abstract void readChunk(ReadableByteChannel ch) throws IOException, IgniteCheckedException;
}
