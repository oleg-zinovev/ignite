package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;

/** Base implementation of window partition */
public abstract class WindowPartitionBase<Row> {
    private final Comparator<Row> grpCmp;
    Comparator<Row> peerCmp;
    private final Supplier<List<WindowFunctionWrapper<Row>>> accFactory;
    private final RowHandler.RowFactory<Row> accRowFactory;

    private Row previousRow;
    private int peerCmpRes;
    private int rowIdx;
    private int peerIdx;

    /** */
    WindowPartitionBase(
        Comparator<Row> grpCmp,
        Comparator<Row> peerCmp,
        Supplier<List<WindowFunctionWrapper<Row>>> accFactory,
        RowHandler.RowFactory<Row> accRowFactory
    ) {
        this.grpCmp = grpCmp;
        this.peerCmp = peerCmp;
        this.accFactory = accFactory;
        this.accRowFactory = accRowFactory;
    }

    /**
     * Ensure that the provided row can be added in the current partition.
     */
    public final boolean shouldReset(Row row) {
        return previousRow != null && grpCmp != null && grpCmp.compare(previousRow, row) != 0;
    }

    /** Adding row to the window partition */
    public final void add(Row row) {
        assert previousRow == null || grpCmp == null || grpCmp.compare(previousRow, row) == 0 : "Trying to add row from another group";

        if (previousRow != null && peerCmp != null) {
            int cmp = peerCmp.compare(previousRow, row);
            if (cmp != 0) {
                if (peerCmpRes != 0) {
                    assert Integer.signum(peerCmpRes) == Integer.signum(cmp) : "Peer rows are not sorted";
                    peerCmpRes = cmp;
                }
                peerIdx++;
            }
        }

        doAdd(row, rowIdx++, peerIdx);
        previousRow = row;
    }

    /** Actually adds row to partition */
    protected abstract void doAdd(Row row, int rowIdx, int peerIdx);

    /** Drains window partition to an output collection */
    public abstract boolean drainTo(RowHandler.RowFactory<Row> factory, Collection<Row> output, boolean early);

    /** Reset current window partition (i.e. on group start) */
    public final void reset() {
        previousRow = null;
        peerCmpRes = 0;
        rowIdx = 0;
        peerIdx = 0;
        resetInternal();
    }

    /** */
    protected abstract void resetInternal();

    /** Creates {@link WindowFunctionWrapper} list */
    final List<WindowFunctionWrapper<Row>> createWrappers() {
        return accFactory.get();
    }

    /** Creates row with window function results */
    protected final Row createResultRow(RowHandler.RowFactory<Row> rowFactory, Row source, Object... results) {
        Row resultsRow = accRowFactory.create(results);
        return rowFactory.handler().concat(source, resultsRow);
    }
}
