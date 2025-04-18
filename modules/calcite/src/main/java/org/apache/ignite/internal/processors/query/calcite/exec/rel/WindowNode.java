package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.window.Framing;
import org.apache.ignite.internal.util.typedef.F;

/** Window support node */
public class WindowNode<Row> extends MemoryTrackingNode<Row> implements SingleNode<Row>, Downstream<Row> {

    /**  */
    private final Comparator<Row> grpComp;

    /**  */
    private final Supplier<Framing<Row>> frameFactory;

    /**  */
    private final RowHandler.RowFactory<Row> rowFactory;

    /**  */
    private Framing<Row> framing;

    /**  */
    private Row prevRow;

    /**  */
    private int grpCmpRes;

    /**  */
    private int requested;

    /**  */
    private int waiting;

    /**  */
    private final Deque<Row> outBuf = new ArrayDeque<>(IN_BUFFER_SIZE);

    public WindowNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        Comparator<Row> grpComp,
        IntFunction<Framing<Row>> frameFactory,
        RowHandler.RowFactory<Row> rowFactory
    ) {
        super(ctx, rowType, DFLT_ROW_OVERHEAD);
        this.grpComp = grpComp;
        this.frameFactory = () -> frameFactory.apply(IN_BUFFER_SIZE);
        this.rowFactory = rowFactory;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0;

        checkState();

        requested = rowsCnt;

        doPush();

        if (waiting == 0) {
            waiting = IN_BUFFER_SIZE;

            source().request(IN_BUFFER_SIZE);
        }
        else if (waiting < 0)
            downstream().end();
    }

    @Override public void push(Row row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting--;

        if (framing != null) {
            int cmp = grpComp == null ? 0 : grpComp.compare(row, prevRow);
            if (cmp == 0) {
                framing.add(row);
                nodeMemoryTracker.onRowAdded(row);
            }
            else {
                if (grpCmpRes == 0)
                    grpCmpRes = cmp;
                else
                    assert Integer.signum(cmp) == Integer.signum(grpCmpRes) : "Input not sorted";

                framing.outputTo(rowFactory, outBuf);

                framing = newFrame(row);

                doPush();
            }
        }
        else
            framing = newFrame(row);

        prevRow = row;

        if (waiting == 0 && requested > 0) {
            waiting = IN_BUFFER_SIZE;

            context().execute(() -> source().request(IN_BUFFER_SIZE), this::onError);
        }
    }

    @Override public void end() throws Exception {
        assert downstream() != null;
        if (waiting < 0) {
            return;
        }

        waiting = -1;

        checkState();

        if (framing != null) {
            framing.outputTo(rowFactory, outBuf);
            framing = null;
        }

        doPush();

        downstream().end();
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        framing = null;
        prevRow = null;
        grpCmpRes = 0;
        nodeMemoryTracker.reset();
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /**  */
    private void doPush() throws Exception {
        while (requested > 0 && !outBuf.isEmpty()) {
            requested--;

            downstream().push(outBuf.poll());
        }
    }

    /**  */
    private Framing<Row> newFrame(Row r) {
        Framing<Row> frn = frameFactory.get();
        frn.add(r);

        nodeMemoryTracker.reset();
        nodeMemoryTracker.onRowAdded(r);

        return frn;
    }

}
