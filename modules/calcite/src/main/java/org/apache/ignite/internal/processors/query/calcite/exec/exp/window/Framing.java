package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/** Base implementation of ROWS/RANGE window frame */
public abstract class Framing<Row> {
    private final Supplier<List<WindowAccumulator<Row>>> accFactory;
    private final List<PeerRow<Row>> buffer;

    private PeerRow<Row> previousRow;
    private int peerCmpRes;

    protected Framing(
        Supplier<List<WindowAccumulator<Row>>> accFactory,
        Comparator<Row> peerCmp,
        int bufferSize
    ) {
        this.accFactory = accFactory;
        buffer = new ArrayList<>(bufferSize);
    }

    /** Adds row to current frame */
    public final void add(Row row) {
        int peerIdx = 0;
        if (previousRow != null) {
            int cmp = compareRowPeer(previousRow.data, row);
            if (cmp == 0) {
                peerIdx = previousRow.peerIdx;
            }
            else {
                if (peerCmpRes != 0) {
                    assert Integer.signum(peerCmpRes) == Integer.signum(cmp) : "Peer rows are not sorted";
                    peerCmpRes = cmp;
                }
                peerIdx = previousRow.peerIdx + 1;
            }
        }

        PeerRow<Row> pRow = new PeerRow<>(row, peerIdx);
        buffer.add(pRow);
        previousRow = pRow;
    }

    /** Calculates window accumulators and adds it to an output collection */
    public void outputTo(RowHandler.RowFactory<Row> factory, Collection<Row> output) {
        int size = buffer.size();

        int windowStartIdx = Integer.MIN_VALUE;
        int windowEndIdx = Integer.MIN_VALUE;
        List<WindowAccumulator<Row>> windowAccumulators = List.of();

        for (int rowIdx = 0; rowIdx < size; rowIdx++) {
            PeerRow<Row> currRow = buffer.get(rowIdx);

            int currWindowStartIdx = getCurrentWindowStartIdx(rowIdx, currRow, buffer);
            assert currWindowStartIdx >= -1 : "Window start idx should be greater than minus 1";

            int currWindowEndIdx = getCurrentWindowEndIdx(rowIdx, currRow, buffer);
            assert currWindowEndIdx >= -1 : "Window end idx should be greater than minus 1";

            if (currWindowStartIdx != windowStartIdx || currWindowEndIdx != windowEndIdx) {
                windowStartIdx = currWindowStartIdx;
                windowEndIdx = currWindowEndIdx;
                windowAccumulators = accFactory.get();

                for (int i = Math.max(currWindowStartIdx, 0); i < buffer.size() && i <= currWindowEndIdx; i++) {
                    PeerRow<Row> row = buffer.get(i);
                    for (WindowAccumulator<Row> acc : windowAccumulators) {
                        acc.add(rowIdx, currRow.peerIdx, i, row.peerIdx, row.data);
                    }
                }
            }

            Object[] accResults = Commons.transform(windowAccumulators, WindowAccumulator::end).toArray();

            Row accRow = factory.create(accResults);
            Row resultRow = factory.handler().concat(currRow.data, accRow);

            output.add(resultRow);
        }
    }

    protected abstract int compareRowPeer(Row row1, Row row2);

    protected abstract int getCurrentWindowStartIdx(int rowIdx, PeerRow<Row> row, List<PeerRow<Row>> buffer);

    protected abstract int getCurrentWindowEndIdx(int rowIdx, PeerRow<Row> row, List<PeerRow<Row>> buffer);

    /** Row with peer index */
    protected static final class PeerRow<Row> {
        private final Row data;
        private final int peerIdx;

        private PeerRow(Row data, int peerIdx) {
            this.data = data;
            this.peerIdx = peerIdx;
        }

        public Row getData() {
            return data;
        }

        public int getPeerIdx() {
            return peerIdx;
        }
    }
}
