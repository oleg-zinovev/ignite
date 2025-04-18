package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Widnow framer for ROWS group.
 * Bound offsets:
 * - zero for current row
 * - null for unbounded
 * - not null for offset
 */
final class RowFraming<Row> extends Framing<Row> {
    private final Function<Row, Integer> lowerBoundOffset;
    private final Function<Row, Integer> upperBoundOffset;

    RowFraming(
        Supplier<List<WindowFunctionWrapper<Row>>> accFactory,
        Function<Row, Integer> lowerBoundOffset,
        Function<Row, Integer> upperBoundOffset,
        int bufferSize
    ) {
        super(accFactory, null, bufferSize);
        this.lowerBoundOffset = lowerBoundOffset;
        this.upperBoundOffset = upperBoundOffset;
    }

    @Override protected int compareRowPeer(Row row1, Row row2) {
        // RowFraming: peer idx equals to row idx.
        // So, row2 is always greater than row1.
        return 1;
    }

    @Override protected int getCurrentWindowStartIdx(int rowIdx, PeerRow<Row> row, List<PeerRow<Row>> buffer) {
        Integer offset = lowerBoundOffset.apply(row.data);
        if (offset == null) {
            return 0;
        }
        return Math.max(rowIdx - offset, -1);
    }

    @Override protected int getCurrentWindowEndIdx(int rowIdx, PeerRow<Row> row, List<PeerRow<Row>> buffer) {
        Integer offset = upperBoundOffset.apply(row.data);
        if (offset == null) {
            return buffer.size();
        }
        return Math.max(rowIdx + offset, -1);
    }

}
