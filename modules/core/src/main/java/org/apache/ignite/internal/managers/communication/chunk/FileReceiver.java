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
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.communication.FileHandler;
import org.apache.ignite.internal.managers.communication.TransmitMeta;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Class represents a chunk data receiver which is pulling data from channel vi
 * {@link FileChannel#transferFrom(ReadableByteChannel, long, long)}.
 */
public class FileReceiver extends AbstractReceiver {
    /** The default factory to provide IO oprations over underlying file. */
    @GridToStringExclude
    private static final FileIOFactory dfltIoFactory = new RandomAccessFileIOFactory();

    /** Handler to notify when a file has been processed. */
    private final FileHandler handler;

    /** The abstract java representation of the chunked file. */
    private File file;

    /** The corresponding file channel to work with. */
    @GridToStringExclude
    private FileIO fileIo;

    /**
     * @param name The unique file name within transfer process.
     * @param startPos The position from which the transfer should start to.
     * @param cnt The number of bytes to expect of transfer.
     * @param params Additional stream params.
     * @param stopChecker Node stop or prcoess interrupt checker.
     * @param handler The file handler to process download result.
     */
    public FileReceiver(
        String name,
        long startPos,
        long cnt,
        Map<String, Serializable> params,
        Supplier<Boolean> stopChecker,
        FileHandler handler
    ) {
        super(name, startPos, cnt, params, stopChecker);

        assert handler != null;

        this.handler = handler;
    }

    /** {@inheritDoc} */
    @Override public void receive(
        ReadableByteChannel ch,
        TransmitMeta meta,
        int chunkSize
    ) throws IOException, IgniteCheckedException {
        super.receive(ch, meta, chunkSize);

        if (transferred == total)
            handler.accept(file);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        U.closeQuiet(fileIo);

        fileIo = null;
    }

    /** {@inheritDoc} */
    @Override protected void init(int chunkSize) throws IgniteCheckedException {
        assert file == null;

        chunkSize(chunkSize);

        String fileAbsPath = handler.path();

        if (fileAbsPath == null)
            throw new IgniteCheckedException("Requested for the chunked stream a file absolute path is incorrect: " + this);

        file = new File(fileAbsPath);
    }

    /** {@inheritDoc} */
    @Override protected void readChunk(ReadableByteChannel ch) throws IOException, IgniteCheckedException {
        try {
            if (fileIo == null) {
                fileIo = dfltIoFactory.create(file);

                fileIo.position(startPos);
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Unable to open file for IO operations. File download will be stopped", e);
        }

        long batchSize = Math.min(chunkSize, total - transferred);

        long readed = fileIo.transferFrom(ch, startPos + transferred, batchSize);

        if (readed > 0)
            transferred += readed;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FileReceiver.class, this, "super", super.toString());
    }
}
