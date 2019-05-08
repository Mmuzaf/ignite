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

package org.apache.ignite.internal.processors.transmit.channel;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class TransmitMeta implements Externalizable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    private static final String DFLT_UNNAMED_META = "null";

    /**
     * The name to associate particular meta with.
     * Can be the particular file name, or an a transfer session identifier.
     */
    private String name;

    /** */
    private long offset;

    /** */
    private long count;

    /** The initial meta info for the file transferred the first time. */
    private boolean initial;

    /** */
    private HashMap<String, Serializable> map = new HashMap<>();

    /**
     *
     */
    public TransmitMeta() {
        this(DFLT_UNNAMED_META);
    }

    /**
     * @param name The name to associate particular meta with.
     */
    public TransmitMeta(String name) {
        this(name, -1, -1, true, null);
    }

    /**
     * @param name The string name representation to assoticate particular meta with.
     * @param offset The start position of file.
     * @param count The amount of bytes to receive.
     * @param initial {@code true} if
     * @param params The additional transfer meta params.
     */
    public TransmitMeta(
        String name,
        long offset,
        long count,
        boolean initial,
        Map<String, Serializable> params
    ) {
        this.name = Objects.requireNonNull(name);
        this.offset = offset;
        this.count = count;
        this.initial = initial;

        if (params != null) {
            for (Map.Entry<String, Serializable> key : params.entrySet())
                map.put(key.getKey(), key.getValue());
        }
    }

    /**
     * The string representation name of particular file.
     */
    public String name() {
        return Objects.requireNonNull(name);
    }

    /**
     * @return The position to start channel transfer at.
     */
    public long offset() {
        return offset;
    }

    /**
     * @return The number of bytes expect to transfer.
     */
    public long count() {
        return count;
    }

    /**
     * @return {@code true} if the file is transferred the first time.
     */
    public boolean initial() {
        return initial;
    }

    /**
     * @return The map of additional keys.
     */
    public Map<String, Serializable> params() {
        return Collections.unmodifiableMap(map);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(name());
        out.writeLong(offset);
        out.writeLong(count);
        out.writeBoolean(initial);
        out.writeObject(map);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        name = Objects.requireNonNull(in.readUTF());
        offset = in.readLong();
        count = in.readLong();
        initial = in.readBoolean();
        map = (HashMap)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        TransmitMeta meta = (TransmitMeta)o;

        return offset == meta.offset &&
            count == meta.count &&
            initial == meta.initial &&
            name.equals(meta.name);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(name, offset, count, initial);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransmitMeta.class, this);
    }
}
