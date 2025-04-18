package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.List;
import java.util.function.Supplier;

/**
 * Widnow framer for ROWS group.
 * Bound offsets:
 * - negative for the preceding rows
 * - zero for current row
 * - positive for the following rows
 * */
public final class RowFraming<Row> extends Framing<Row> {
    private final int lowerBoundOffset;
    private final int upperBoundOffset;

    RowFraming(
        Supplier<List<WindowAccumulator<Row>>> accFactory,
        int lowerBoundOffset,
        int upperBoundOffset,
        int bufferSize
    ) {
        super(accFactory, null, bufferSize);
        this.lowerBoundOffset = lowerBoundOffset;
        this.upperBoundOffset = upperBoundOffset;
    }

    @Override protected int compareRowPeer(Row row1, Row row2) {
        // RowFraming: peer idx equals to row idx.
        // So, row2 is always greater when row1.
        return 1;
    }

    @Override protected int getCurrentWindowStartIdx(int rowIdx, PeerRow<Row> row, List<PeerRow<Row>> buffer) {
        return Math.max(rowIdx + lowerBoundOffset, -1);
    }

    @Override protected int getCurrentWindowEndIdx(int rowIdx, PeerRow<Row> row, List<PeerRow<Row>> buffer) {
        return Math.max(rowIdx + upperBoundOffset, -1);
    }

}
