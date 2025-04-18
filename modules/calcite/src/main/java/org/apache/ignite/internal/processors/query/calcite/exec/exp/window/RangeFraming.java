package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/** Widnow framer for ROWS group */
public final class RangeFraming<Row> extends Framing<Row> {
    /**  */
    private final Comparator<Row> peerCmp;

    /**  */
    private final Function<Row, Row> rangeLowerBound;

    /**  */
    private final Function<Row, Row> rangeUpperBound;

    RangeFraming(
        Supplier<List<WindowAccumulator<Row>>> accFactory,
        Comparator<Row> peerCmp,
        Function<Row, Row> rangeLowerBound,
        Function<Row, Row> rangeUpperBound,
        int bufferSize
    ) {
        super(accFactory, peerCmp, bufferSize);
        this.peerCmp = peerCmp;
        this.rangeLowerBound = rangeLowerBound;
        this.rangeUpperBound = rangeUpperBound;
    }

    @Override protected int compareRowPeer(Row row1, Row row2) {
        if (peerCmp == null) {
            // in case peerCmp is not set - all rows in one peer
            return 0;
        }
        else {
            return peerCmp.compare(row1, row2);
        }
    }

    @Override protected int getCurrentWindowStartIdx(int rowIdx, PeerRow<Row> row, List<PeerRow<Row>> buffer) {
        Row lowerBoundRow = rangeLowerBound.apply(row.getData());
        if (lowerBoundRow == null)
            return 0;
        return bsearchLowerBound(lowerBoundRow, buffer);
    }

    @Override protected int getCurrentWindowEndIdx(int rowIdx, PeerRow<Row> row, List<PeerRow<Row>> buffer) {
        Row upperBoundRow = rangeUpperBound.apply(row.getData());
        if (upperBoundRow == null)
            return buffer.size();
        return bsearchUpperBound(upperBoundRow, buffer);
    }

    private int bsearchLowerBound(Row row, List<PeerRow<Row>> buffer) {
        int start = 0;
        int end = buffer.size() - 1;

        while (start <= end) {
            int mid = (start + end) / 2;

            Row midRow = buffer.get(mid).getData();
            int cmp = compareRowPeer(midRow, row);

            if (cmp == 0) {
                end = mid - 1;
            }
            else if (cmp > 0)
                end = mid - 1;
            else
                start = mid + 1;
        }

        return start;
    }

    private int bsearchUpperBound(Row row, List<PeerRow<Row>> buffer) {
        int start = 0;
        int end = buffer.size() - 1;

        while (start <= end) {
            int mid = (start + end) / 2;

            Row midRow = buffer.get(mid).getData();
            int cmp = compareRowPeer(midRow, row);

            if (cmp == 0) {
                start = mid + 1;
            }
            else if (cmp > 0)
                end = mid - 1;
            else
                start = mid + 1;
        }

        return end;
    }
}
