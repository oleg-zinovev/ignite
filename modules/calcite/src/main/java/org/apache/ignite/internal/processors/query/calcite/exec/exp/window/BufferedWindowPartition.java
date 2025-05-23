package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.GridIntList;

/** Buffering implementation of the ROWS / RANGE window partition */
final class BufferedWindowPartition<Row> extends WindowPartitionBase<Row> {
    private final List<Row> buffer;
    // holds an end row index for each peer index.
    private final GridIntList row2peerIdx;
    // frame within partition.
    private final WindowFunctionFrame<Row> frame;

    // cache for peer index search.
    private int lastRowIdx = -1;
    private int lastPeerIdx = -1;

    /**  */
    BufferedWindowPartition(
        Comparator<Row> grpCmp,
        Comparator<Row> peerCmp,
        Supplier<List<WindowFunctionWrapper<Row>>> accFactory,
        RowHandler.RowFactory<Row> accRowFactory,
        ExecutionContext<Row> ctx,
        Window.Group group,
        RelDataType inputRowType
    ) {
        super(grpCmp, peerCmp, accFactory, accRowFactory);
        buffer = new ArrayList<>();
        row2peerIdx = new GridIntList();
        frame = createFrame(ctx, peerCmp, group, inputRowType, buffer);
    }

    /** {@inheritDoc} */
    @Override protected void doAdd(Row row, int rowIdx, int peerIdx) {
        buffer.add(row);
        assert rowIdx >= 0 : "Row idx should be greater or equal to zero";
        assert peerIdx == row2peerIdx.size() || peerIdx == row2peerIdx.size() - 1 : "Peer idx not monotonically increasing";
        if (peerIdx == row2peerIdx.size() - 1)
            row2peerIdx.set(peerIdx, rowIdx);
        else {
            row2peerIdx.add(rowIdx);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean drainTo(RowHandler.RowFactory<Row> factory, Collection<Row> output, boolean early) {
        if (early || buffer.isEmpty()) {
            return false;
        }

        List<WindowFunctionWrapper<Row>> windowAccumulators = createWrappers();

        int size = buffer.size();
        for (int rowIdx = 0; rowIdx < size; rowIdx++) {
            Row currRow = buffer.get(rowIdx);
            int peerIdx = findPeerIdx(rowIdx);

            for (WindowFunctionWrapper<Row> acc : windowAccumulators) {
                acc.add(currRow, rowIdx, peerIdx, frame);
            }

            Object[] accResults = Commons.transform(windowAccumulators, WindowFunctionWrapper::end).toArray();
            Row resultRow = createResultRow(factory, currRow, accResults);
            output.add(resultRow);
        }

        reset();
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void resetInternal() {
        buffer.clear();
        row2peerIdx.clear();
        frame.reset();
        lastRowIdx = -1;
        lastPeerIdx = -1;
    }

    /** Searches peer index by row index */
    private int findPeerIdx(int idx) {
        if (lastRowIdx != idx) {
            int pos = row2peerIdx.bsearch(idx);
            if (pos < 0) {
                pos = -(pos + 1);
            }
            lastRowIdx = idx;
            lastPeerIdx = pos;
        }
        return lastPeerIdx;
    }

    /** Creates frame for partition */
    private static <Row> WindowFunctionFrame<Row> createFrame(
        ExecutionContext<Row> ctx,
        Comparator<Row> peerCmp,
        Window.Group group,
        RelDataType inputRowType,
        List<Row> buffer
    ) {
        if (group.isRows)
            return new RowWindowPartitionIndex<>(buffer, ctx, group, inputRowType);
        else
            return new RangeWindowPartitionIndex<>(buffer, ctx, peerCmp, group, inputRowType);
    }
}
