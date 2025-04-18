package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/** */
public final class WindowAccumulatorsFactory<Row> implements Supplier<List<WindowAccumulator<Row>>> {
    private final Supplier<List<AccumulatorWrapper<Row>>> accFactory;

    /** */
    public WindowAccumulatorsFactory(
        ExecutionContext<Row> ctx,
        List<AggregateCall> aggCalls,
        RelDataType inputRowType
    ) {
        //todo: handle special cass aggregate call (e.g. dense_rank, row_number).
        accFactory = ctx.expressionFactory().accumulatorsFactory(AggregateType.SINGLE, aggCalls, inputRowType);
    }

    @Override public List<WindowAccumulator<Row>> get() {
        return Commons.transform(accFactory.get(), WindowAccumulatorAdapter::new);
    }

    /** */
    private static final class WindowAccumulatorAdapter<Row> implements WindowAccumulator<Row> {
        private final AccumulatorWrapper<Row> acc;

        private WindowAccumulatorAdapter(AccumulatorWrapper<Row> acc) {
            this.acc = acc;
        }

        @Override public void add(int currRowIdx, int currPeerIdx, int rowIdx, int peerIdx, Row row) {
            acc.add(row);
        }

        @Override public Object end() {
            return acc.end();
        }
    }
}
