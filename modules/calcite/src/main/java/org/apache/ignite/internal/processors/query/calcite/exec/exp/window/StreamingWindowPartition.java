package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/** Non-buffering implementation of the ROWS / RANGE window partition */
final class StreamingWindowPartition<Row> extends WindowPartitionBase<Row> {
    private Row row;
    private int rowIdx;
    private int peerIdx;
    private List<WindowFunctionWrapper<Row>> accumulators;

    /** */
    StreamingWindowPartition(
        Comparator<Row> grpCmp,
        Comparator<Row> peerCmp,
        Supplier<List<WindowFunctionWrapper<Row>>> accFactory,
        RowHandler.RowFactory<Row> accRowFactory
    ) {
        super(grpCmp, peerCmp, accFactory, accRowFactory);
    }

    /** {@inheritDoc} */
    @Override protected void doAdd(Row row, int rowIdx, int peerIdx) {
        assert this.row == null : "StreamingWindowPartition can only hold one row";
        this.row = row;
        this.rowIdx = rowIdx;
        this.peerIdx = peerIdx;
    }

    /** {@inheritDoc} */
    @Override public boolean drainTo(RowHandler.RowFactory<Row> factory, Collection<Row> output, boolean early) {
        if (row == null) {
            return false;
        }

        if (accumulators == null) {
            accumulators = createWrappers();
        }

        for (WindowFunctionWrapper<Row> acc : accumulators) {
            acc.add(row, rowIdx, peerIdx);
        }

        Object[] accResults = Commons.transform(accumulators, WindowFunctionWrapper::end).toArray();

        Row resultRow = createResultRow(factory, row, accResults);
        output.add(resultRow);

        row = null;
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void resetInternal() {
        row = null;
        rowIdx = 0;
        accumulators = null;
    }
}
