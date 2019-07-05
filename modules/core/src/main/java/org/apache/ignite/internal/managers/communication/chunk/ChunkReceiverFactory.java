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

import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.communication.TransmissionHandler;
import org.apache.ignite.internal.managers.communication.TransmitMeta;

/**
 * Factory which produces a chunk data receivers.
 */
@FunctionalInterface
public interface ChunkReceiverFactory {
    /**
     * @param nodeId Remote node id.
     * @param handler The current handler instance which produces chunk recevier handlers.
     * @param meta Meta information of input data.
     * @param checker Process stopped checker.
     * @return Chunk receiver instance.
     * @throws IgniteCheckedException If fails.
     */
    public AbstractReceiver create(
        UUID nodeId,
        TransmissionHandler handler,
        TransmitMeta meta,
        Supplier<Boolean> checker
    ) throws IgniteCheckedException;
}
