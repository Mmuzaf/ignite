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
package org.apache.ignite.internal.processors.cache.verify;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Encapsulates result of {@link VerifyBackupPartitionsTaskV2}.
 */
public class IdleVerifyResultV2 extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Counter conflicts. */
    @GridToStringInclude
    private Map<PartitionKeyV2, List<PartitionHashRecordV2>> cntrConflicts;

    /** Hash conflicts. */
    @GridToStringInclude
    private Map<PartitionKeyV2, List<PartitionHashRecordV2>> hashConflicts;

    /** Moving partitions. */
    @GridToStringInclude
    private Map<PartitionKeyV2, List<PartitionHashRecordV2>> movingPartitions;

    /** Lost partitions. */
    @GridToStringInclude
    private Map<PartitionKeyV2, List<PartitionHashRecordV2>> lostPartitions;

    /** Exceptions. */
    @GridToStringInclude
    private Map<ClusterNode, Exception> exceptions;

    /**
     * @param cntrConflicts Counter conflicts.
     * @param hashConflicts Hash conflicts.
     * @param movingPartitions Moving partitions.
     * @param exceptions Occurred exceptions.
     */
    public IdleVerifyResultV2(
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> cntrConflicts,
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> hashConflicts,
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> movingPartitions,
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> lostPartitions,
        Map<ClusterNode, Exception> exceptions
    ) {
        this.cntrConflicts = cntrConflicts;
        this.hashConflicts = hashConflicts;
        this.movingPartitions = movingPartitions;
        this.lostPartitions = lostPartitions;
        this.exceptions = exceptions;
    }

    /**
     * @param exceptions Occurred exceptions.
     */
    public IdleVerifyResultV2(Map<ClusterNode, Exception> exceptions) {
        this(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), exceptions);
    }

    /**
     * Default constructor for Externalizable.
     */
    public IdleVerifyResultV2() {
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V3;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, cntrConflicts);
        U.writeMap(out, hashConflicts);
        U.writeMap(out, movingPartitions);
        U.writeMap(out, exceptions);
        U.writeMap(out, lostPartitions);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        cntrConflicts = U.readMap(in);
        hashConflicts = U.readMap(in);
        movingPartitions = U.readMap(in);

        if (protoVer >= V2)
            exceptions = U.readMap(in);

        if (protoVer >= V3)
            lostPartitions = U.readMap(in);
    }

    /**
     * @return Counter conflicts.
     */
    public Map<PartitionKeyV2, List<PartitionHashRecordV2>> counterConflicts() {
        return cntrConflicts;
    }

    /**
     * @return Hash conflicts.
     */
    public Map<PartitionKeyV2, List<PartitionHashRecordV2>> hashConflicts() {
        return hashConflicts;
    }

    /**
     * @return Moving partitions.
     */
    public Map<PartitionKeyV2, List<PartitionHashRecordV2>> movingPartitions() {
        return Collections.unmodifiableMap(movingPartitions);
    }

    /**
     * @return Lost partitions.
     */
    public Map<PartitionKeyV2, List<PartitionHashRecordV2>> lostPartitions() {
        return lostPartitions;
    }

    /**
     * @return {@code true} if any conflicts were discovered during the check.
     */
    public boolean hasConflicts() {
        return !F.isEmpty(hashConflicts()) || !F.isEmpty(counterConflicts());
    }

    /**
     * @return Exceptions on nodes.
     */
    public Map<ClusterNode, Exception> exceptions() {
        return exceptions;
    }

    /**
     * Print formatted result to the given printer.
     *
     * @param printer Consumer for handle formatted result.
     * @param printExceptionMessages {@code true} if exceptions must be included too.
     */
    public void print(Consumer<String> printer, boolean printExceptionMessages) {
        boolean noMatchingCaches = false;

        boolean succeeded = true;

        for (Exception e : exceptions.values()) {
            if (e instanceof NoMatchingCachesException) {
                noMatchingCaches = true;
                succeeded = false;

                break;
            }
        }

        if (succeeded) {
            if (!F.isEmpty(exceptions)) {
                int size = exceptions.size();

                printer.accept("The check procedure failed on " + size + " node" + (size == 1 ? "" : "s") + ".\n");
            }

            if (!hasConflicts())
                printer.accept("The check procedure has finished, no conflicts have been found.\n");
            else
                printConflicts(printer);

            Map<PartitionKeyV2, List<PartitionHashRecordV2>> moving = movingPartitions();

            if (!moving.isEmpty())
                printer.accept("Possible results are not full due to rebalance still in progress." + U.nl());

            printSkippedPartitions(printer, moving, "MOVING");
            printSkippedPartitions(printer, lostPartitions(), "LOST");
        }
        else {
            printer.accept("\nThe check procedure failed.\n");

            if (noMatchingCaches)
                printer.accept("\nThere are no caches matching given filter options.\n");
        }

        if (!F.isEmpty(exceptions())) {
            printer.accept("\nThe check procedure failed on nodes:\n");

            for (Map.Entry<ClusterNode, Exception> e : exceptions().entrySet()) {
                ClusterNode n = e.getKey();

                printer.accept("\nNode ID: " + n.id() + " " + n.addresses() + "\nConsistent ID: " + n.consistentId() + "\n");

                if (printExceptionMessages) {
                    String msg = e.getValue().getMessage();

                    printer.accept("Exception: " + e.getValue().getClass().getCanonicalName() + "\n");
                    printer.accept(msg == null ? "" : msg + "\n");

                    StringWriter writer = new StringWriter();

                    e.getValue().printStackTrace(new PrintWriter(writer));

                    printer.accept("With stacktrace: " + writer.toString());
                }
            }
        }
    }

    /**
     * Print partitions which were skipped.
     *
     * @param printer Consumer for printing.
     * @param map Partitions storage.
     * @param partitionState Partition state.
     */
    private void printSkippedPartitions(
        Consumer<String> printer,
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> map,
        String partitionState
    ) {
        if (!F.isEmpty(map)) {
            printer.accept("Verification was skipped for " + map.size() + " " + partitionState + " partitions:\n");

            for (Map.Entry<PartitionKeyV2, List<PartitionHashRecordV2>> entry : map.entrySet()) {
                printer.accept("Skipped partition: " + entry.getKey() + "\n");

                printer.accept("Partition instances: " + entry.getValue() + "\n");
            }
        }
    }

    /** */
    private void printConflicts(Consumer<String> printer) {
        int cntrConflictsSize = counterConflicts().size();
        int hashConflictsSize = hashConflicts().size();

        printer.accept("The check procedure has finished, found " + (cntrConflictsSize + hashConflictsSize) +
            " conflict partitions: [counterConflicts=" + cntrConflictsSize + ", hashConflicts=" +
            hashConflictsSize + "]\n");

        if (!F.isEmpty(counterConflicts())) {
            printer.accept("Update counter conflicts:\n");

            for (Map.Entry<PartitionKeyV2, List<PartitionHashRecordV2>> entry : counterConflicts().entrySet()) {
                printer.accept("Conflict partition: " + entry.getKey() + "\n");

                printer.accept("Partition instances: " + entry.getValue() + "\n");
            }
        }

        if (!F.isEmpty(hashConflicts())) {
            printer.accept("Hash conflicts:\n");

            for (Map.Entry<PartitionKeyV2, List<PartitionHashRecordV2>> entry : hashConflicts().entrySet()) {
                printer.accept("Conflict partition: " + entry.getKey() + "\n");

                printer.accept("Partition instances: " + entry.getValue() + "\n");
            }
        }

        printer.accept("\n");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IdleVerifyResultV2.class, this);
    }
}
