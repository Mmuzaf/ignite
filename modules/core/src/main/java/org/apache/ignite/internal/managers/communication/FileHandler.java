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

/**
 * The {@code FileHandler} represents by itself the way of input data stream processing. All the data will
 * be processed under the hood using zero-copy transferring algorithm and only start file processing and
 * the end of processing will be provided.
 */
public interface FileHandler {
    /**
     * @return The absolute pathname string denoting the file.
     */
    public String path();

    /**
     * @param file File with fully downloaded data into.
     */
    public void accept(File file);
}
