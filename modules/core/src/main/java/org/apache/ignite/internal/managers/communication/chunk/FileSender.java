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

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.managers.communication.ReadPolicy;
import org.apache.ignite.internal.managers.communication.TransmitMeta;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Class represents a sender of chunked data which can be pushed to channel.
 * Supports the zero-copy streaming algorithm,  see {@link FileChannel#transferTo(long, long, WritableByteChannel)}
 * for details.
 */
public class FileSender extends AbstractProcess {
    /** The default factory to provide IO oprations over underlying file. */
    @GridToStringExclude
    private static final FileIOFactory dfltIoFactory = new RandomAccessFileIOFactory();

    /** The abstract java representation of the chunked file. */
    private final File file;

    /** The corresponding file channel to work with. */
    @GridToStringExclude
    private FileIO fileIo;

    /**
     * @param file File representation of current object.
     * @param pos File offset.
     * @param cnt Number of bytes to transfer.
     * @param params Additional file params.
     * @param stopChecker Node stop or prcoess interrupt checker.
     * @param chunkSize The size of chunk to read.
     */
    public FileSender(
        File file,
        long pos,
        long cnt,
        Map<String, Serializable> params,
        Supplier<Boolean> stopChecker,
        int chunkSize
    ) {
        super(file.getName(), pos, cnt, params, stopChecker);

        assert file != null;

        this.file = file;

        chunkSize(chunkSize);
    }

    /** {@inheritDoc} */
    @Override public void transferred(long cnt) {
        try {
            if (fileIo != null)
                fileIo.position(startPosition() + cnt);

            super.transferred(cnt);
        }
        catch (IOException e) {
            throw new IgniteException("Unable to set new start file channel position [pos=" + (startPosition() + cnt) + ']');
        }
    }

    /**
     * @param oo Channel to write data to.
     * @param uploadedBytes Number of bytes transferred on previous attempt.
     * @param plc Policy of way how data will be handled on remote node.
     * @throws IOException If write meta input failed.
     * @throws IgniteCheckedException If fails.
     */
    public void setup(
        ObjectOutput oo,
        long uploadedBytes,
        ReadPolicy plc
    ) throws IOException, IgniteCheckedException {
        transferred(uploadedBytes);

        TransmitMeta meta = new TransmitMeta(name(),
            startPosition() + transferred(),
            count(),
            transferred() == 0,
            params(),
            plc,
            null,
            null);

        meta.writeExternal(oo);

        try {
            fileIo = dfltIoFactory.create(file);

            fileIo.position(startPosition());
        }
        catch (IOException e) {
            // Consider this IO exeption as a user one (not the network exception) and interrupt upload process.
            throw new IgniteCheckedException("Unable to initialize a file IO. File upload will be interrupted", e);
        }
    }

    /**
     * @param ch Output channel to write data to.
     * @throws IOException If an io exception occurred.
     * @throws IgniteCheckedException If fails.
     */
    public void send(WritableByteChannel ch) throws IOException, IgniteCheckedException {
        assert fileIo != null : "Write operation stopped. Chunked object is not initialized";

        while (hasNextChunk()) {
            if (Thread.currentThread().isInterrupted() || stopped()) {
                throw new IgniteCheckedException("Thread has been interrupted or operation has been cancelled " +
                    "due to node is stopping. Channel processing has been stopped.");
            }

            writeChunk(ch);
        }

        checkTransferLimitCount();
    }

    /**
     * @param ch Channel to write data into.
     * @throws IOException If fails.
     */
    private void writeChunk(WritableByteChannel ch) throws IOException {
        long batchSize = Math.min(chunkSize(), count() - transferred());

        long sent = fileIo.transferTo(startPosition() + transferred(), batchSize, ch);

        if (sent > 0)
            transferred += sent;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        U.closeQuiet(fileIo);

        fileIo = null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FileSender.class, this, "super", super.toString());
    }
}
