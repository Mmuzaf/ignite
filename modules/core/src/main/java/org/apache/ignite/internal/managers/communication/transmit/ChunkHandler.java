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

package org.apache.ignite.internal.managers.communication.transmit;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The {@code ChunkHandler} represents by itself the way of input data stream processing.
 * It accepts within each chunk a {@link ByteBuffer} with data from input for further processing.
 */
public interface ChunkHandler {
    /**
     * @return The size of of {@link ByteBuffer} to read the input channel into.
     */
    public int size();

    /**
     * @param buff The data filled buffer.
     * @throws IOException If fails.
     */
    public void accept(ByteBuffer buff) throws IOException;

    /**
     * Chunked handler finishes.
     */
    public void end();
}
