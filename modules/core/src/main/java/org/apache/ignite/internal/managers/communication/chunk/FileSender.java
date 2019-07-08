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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.managers.communication.ReadPolicy;
import org.apache.ignite.internal.managers.communication.TransmitMeta;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.IgniteUtils.assertParameter;

/**
 * Class represents a sender of chunked data which can be pushed to channel.
 * Supports the zero-copy streaming algorithm,  see {@link FileChannel#transferTo(long, long, WritableByteChannel)}
 * for details.
 */
public class FileSender extends AbstractTransferer {
    /** Ignite logger. */
    private final IgniteLogger log;

    /** The default factory to provide IO oprations over underlying file. */
    @GridToStringExclude
    private final FileIOFactory fileIoFactory;

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
     * @param log Ignite logger.
     * @param factory Factory to produce IO interface on files.
     * @param chunkSize The size of chunk to read.
     */
    public FileSender(
        File file,
        long pos,
        long cnt,
        Map<String, Serializable> params,
        Supplier<Boolean> stopChecker,
        IgniteLogger log,
        FileIOFactory factory,
        int chunkSize
    ) {
        super(file.getName(), pos, cnt, params, stopChecker);

        assert file != null;
        assert chunkSize > 0;

        this.file = file;
        this.chunkSize = chunkSize;
        fileIoFactory = factory;
        this.log = log.getLogger(FileSender.class);
    }

    /**
     * @param ch Output channel to write data to.
     * @param oo Channel to write data to.
     * @param connMeta Meta received on connection established.
     * @param plc Policy of way how data will be handled on remote node.
     * @throws IOException If an io exception occurred.
     * @throws IgniteCheckedException If fails.
     */
    public void send(WritableByteChannel ch,
        ObjectOutput oo,
        @Nullable TransmitMeta connMeta,
        ReadPolicy plc
    ) throws IOException, IgniteCheckedException {
        try {
            if (fileIo == null) {
                fileIo = fileIoFactory.create(file);

                fileIo.position(startPos);
            }
        }
        catch (IOException e) {
            // Consider this IO exeption as a user one (not the network exception) and interrupt upload process.
            throw new IgniteCheckedException("Unable to initialize source file. File  sender upload will be stopped", e);
        }

        // If not the initial connection for the current session.
        if (connMeta != null)
            setState(connMeta);

        // Send meta about curent file to remote.
        new TransmitMeta(name,
            startPos + transferred,
            total - transferred,
            transferred == 0,
            params,
            plc,
            null,
            null)
            .writeExternal(oo);

        oo.flush();

        while (hasNextChunk()) {
            if (Thread.currentThread().isInterrupted() || stopped()) {
                throw new IgniteCheckedException("Thread has been interrupted or operation has been cancelled " +
                    "due to node is stopping. Channel processing has been stopped.");
            }

            writeChunk(ch);
        }

        assertTransferredBytes();
    }

    /**
     * @param connMeta Meta file information about
     * @throws IgniteCheckedException If fails.
     */
    private void setState(TransmitMeta connMeta) throws IgniteCheckedException {
        assert connMeta != null;
        assert fileIo != null;

        if (connMeta.initial())
            return;

        long uploadedBytes = connMeta.offset() - startPos;

        assertParameter(name.equals(connMeta.name()), "Attempt to transfer different file " +
            "while previous is not completed [curr=" + name + ", meta=" + connMeta + ']');

        assertParameter(uploadedBytes >= 0, "Incorrect sync meta [offset=" + connMeta.offset() +
            ", startPos=" + startPos + ']');

        // No need to set new file position, if it is not changed.
        if (uploadedBytes == 0)
            return;

        try {
            fileIo.position(startPos + uploadedBytes);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Unable to set new start file channel position " +
                "[pos=" + (startPos + uploadedBytes) + ']', e);
        }

        transferred = uploadedBytes;

        U.log(log, "Update senders number of transferred bytes after reconnect: " + uploadedBytes);
    }

    /**
     * @param ch Channel to write data into.
     * @throws IOException If fails.
     */
    private void writeChunk(WritableByteChannel ch) throws IOException {
        long batchSize = Math.min(chunkSize, total - transferred);

        long sent = fileIo.transferTo(startPos + transferred, batchSize, ch);

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
