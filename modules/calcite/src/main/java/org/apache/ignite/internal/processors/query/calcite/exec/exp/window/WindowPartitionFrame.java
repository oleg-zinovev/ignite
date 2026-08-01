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

package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.Comparator;
import java.util.List;
import org.apache.calcite.rex.RexWindowExclusion;

/** Rows frame within window partition. */
abstract class WindowPartitionFrame<Row> {
    /** Holds immutable refrence to buffered window partition rows. */
    private final List<Row> buf;

    /** Comparator for determining a peer's index within a partition. */
    private final Comparator<Row> peerCmp;

    /** Window group exclusion. */
    final RexWindowExclusion exclusion;

    /**  */
    WindowPartitionFrame(List<Row> buf, Comparator<Row> peerCmp, RexWindowExclusion exclusion) {
        this.buf = buf;
        this.peerCmp = peerCmp;
        this.exclusion = exclusion;
    }

    /** Returns row from partition by index. */
    Row get(int idx) {
        assert idx >= 0 && idx < buf.size() : "Invalid row index";
        return buf.get(idx);
    }

    /** Returns start frame index in partition for current row peer. */
    abstract int getFrameStart(int rowIdx, int peerIdx);

    /** Returns end frame index in partition for current row peer. */
    abstract int getFrameEnd(int rowIdx, int peerIdx);

    /** Returns frame size in partition for the current row peer. */
    final int size(int rowIdx, int peerIdx) {
        int start = getFrameStart(rowIdx, peerIdx);
        int end = getFrameEnd(rowIdx, peerIdx);
        if (end >= start)
            return end - start + 1;
        else
            return 0;
    }

    /** Returns row count in partition. */
    final int size() {
        return buf.size();
    }

    /** Compares two rows using peer comparator. */
    final int compareRowPeer(Row row1, Row row2) {
        // in case peerCmp is not set - all rows has one peer
        return peerCmp == null ? 0 : peerCmp.compare(row1, row2);
    }

    /** Checks if candidate row should be excluded from frame againts current row. */
    public boolean exclude(int currRowIdx, int candidateRowIdx) {
        switch (exclusion) {
            case EXCLUDE_CURRENT_ROW:
                return currRowIdx == candidateRowIdx;
            case EXCLUDE_TIES:
                return currRowIdx != candidateRowIdx && compareRowPeer(get(currRowIdx), get(candidateRowIdx)) == 0;
            case EXCLUDE_GROUP:
                return compareRowPeer(get(currRowIdx), get(candidateRowIdx)) == 0;
            case EXCLUDE_NO_OTHER:
            default:
                return false;
        }
    }
}
