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

package org.apache.ignite.internal.processors.transmit;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;

/**
 * The statefull read file handler from the remote channel.
 */
public interface FileReadHandler {
    /**
     * @param nodeId The remote node id connected from.
     * @param sessionId The unique session id.
     * @param fut The future will be compelted when all files are uploaded.
     */
    public void init(UUID nodeId, String sessionId, IgniteInternalFuture<?> fut);

    /**
     * @param name The file name transfer from.
     * @param keys The additional transfer file description keys.
     * @return The destination object to transfer data to. Can be the {@link File} or {@link ByteBuffer}.
     * @throws IgniteCheckedException If fails.
     */
    public Object begin(String name, Map<String, String> keys) throws IgniteCheckedException;

    /**
     * @param piece The piece of data readed from source.
     * @param piecePos The position particular piece in the original source.
     * @param pieceSize The number of bytes readed from source.
     */
    public void acceptPiece(Object piece, long piecePos, long pieceSize);

    /**
     * @param position The start position pointer of download object in original source.
     * @param count Total count of bytes readed from the original source.
     */
    public void end(long position, long count);

    /**
     * @param cause The case of fail handling process.
     */
    public void exceptionCaught(Throwable cause);
}
