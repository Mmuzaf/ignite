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

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.util.IgniteUtils.assertParameter;

/**
 * Class represents a chunk data receiver which is pulling data from channel vi
 * {@link FileChannel#transferFrom(ReadableByteChannel, long, long)}.
 */
class FileReceiver extends AbstractReceiver {
    /** The default factory to provide IO oprations over underlying file. */
    @GridToStringExclude
    private final FileIOFactory fileIoFactory;

    /** Handler to notify when a file has been processed. */
    private final FileHandler hnd;

    /** The abstract java representation of the chunked file. */
    private File file;

    /** The corresponding file channel to work with. */
    @GridToStringExclude
    private FileIO fileIo;

    /**
     * @param nodeId The remote node id receive request for transmission from.
     * @param initMeta Initial file meta info.
     * @param stopChecker Node stop or prcoess interrupt checker.
     * @param factory Factory to produce IO interface on files.
     * @param hnd Transmission handler to process download result.
     * @throws IgniteCheckedException If fails.
     */
    public FileReceiver(
        UUID nodeId,
        TransmissionMeta initMeta,
        int chunkSize,
        Supplier<Boolean> stopChecker,
        FileIOFactory factory,
        TransmissionHandler hnd
    ) throws IgniteCheckedException {
        super(initMeta, stopChecker);

        assert chunkSize > 0;
        assert initMeta.policy() == TransmissionPolicy.FILE : initMeta.policy();

        this.chunkSize = chunkSize;
        fileIoFactory = factory;
        this.hnd = hnd.fileHandler(nodeId, name(), offset(), count(), params());

        assert this.hnd != null : "FileHandler must be provided by transmission handler";

        String fileAbsPath = this.hnd.path();

        if (fileAbsPath == null || fileAbsPath.trim().isEmpty())
            throw new IgniteCheckedException("File receiver absolute path cannot be empty or null. Receiver cannot be" +
                " initialized: " + this);

        file = new File(fileAbsPath);
    }

    /** {@inheritDoc} */
    @Override public void receive(
        ReadableByteChannel ch,
        TransmissionMeta meta
    ) throws IOException, IgniteCheckedException {
        super.receive(ch, meta);

        if (transferred == count())
            hnd.accept(file);
    }

    /** {@inheritDoc} */
    @Override protected void init(TransmissionMeta meta) throws IgniteCheckedException {
        assert meta != null;
        assert fileIo == null;

        assertParameter(!meta.initial() || meta.name().equals(name()), "Read operation stopped. " +
            "Attempt to receive a new file from channel, while the previous was not fully loaded " +
            "[meta=" + meta + ", prevFile=" + name() + ']');

        try {
            fileIo = fileIoFactory.create(file);

            fileIo.position(offset() + transferred);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Unable to open destination file. Receiver will will be stopped", e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void readChunk(ReadableByteChannel ch) throws IOException, IgniteCheckedException {
        long batchSize = Math.min(chunkSize, count() - transferred);

        long readed = fileIo.transferFrom(ch, offset() + transferred, batchSize);

        if (readed > 0)
            transferred += readed;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        U.closeQuiet(fileIo);

        fileIo = null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FileReceiver.class, this, "super", super.toString());
    }
}
