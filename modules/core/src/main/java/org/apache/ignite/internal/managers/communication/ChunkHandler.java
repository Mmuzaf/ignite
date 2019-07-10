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

import java.io.Closeable;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;

/**
 * The {@code ChunkHandler} represents by itself the way of input data stream processing.
 * It accepts within each chunk a {@link ByteBuffer} with data from input for further processing.
 */
public interface ChunkHandler extends Closeable {
    /**
     * @param pos New offset position in file.
     * @throws IgniteCheckedException If fails to open.
     */
    public void open(long pos) throws IgniteCheckedException;

    /**
     * @return The size of of {@link ByteBuffer} to read the input channel into.
     */
    public int size();

    /**
     * @param buff The data filled buffer.
     * @throws IgniteCheckedException If fails.
     */
    public void accept(ByteBuffer buff) throws IgniteCheckedException;
}
