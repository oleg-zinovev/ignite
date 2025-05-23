package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * Factory to create {@link WindowPartitionBase} factory from {@link Window.Group}
 */
public final class WindowPartitionFactory<Row> implements Supplier<WindowPartitionBase<Row>> {

    private final Supplier<WindowPartitionBase<Row>> supplier;

    public WindowPartitionFactory(
        ExecutionContext<Row> ctx,
        Window.Group group,
        List<AggregateCall> calls,
        RelDataType inputRowType,
        boolean streaming
    ) {
        supplier = () -> {
            List<RelDataType> aggTypes = Commons.transform(calls, AggregateCall::getType);
            RowHandler.RowFactory<Row> aggRowFactory = ctx.rowHandler().factory(Commons.typeFactory(), aggTypes);

            RelCollation grpCollation = TraitUtils.createCollation(group.keys.asList());
            Comparator<Row> grpCmp = ctx.expressionFactory().comparator(grpCollation);
            Comparator<Row> peerCmp;
            if (group.isRows)
                // peer comparator in meaningless in rows frame.
                peerCmp = null;
            else
                peerCmp = ctx.expressionFactory().comparator(group.collation());

            WindowFunctionFactory<Row> accFactory = new WindowFunctionFactory<>(ctx, group, calls, inputRowType);
            assert !(streaming && accFactory.requiresBuffering()) : "Streaming window partition desired, but buffering is required";
            if (accFactory.requiresBuffering())
                return new BufferedWindowPartition<>(grpCmp, peerCmp, accFactory, aggRowFactory, ctx, group, inputRowType);
            else
                return new StreamingWindowPartition<>(grpCmp, peerCmp, accFactory, aggRowFactory);
        };
    }

    @Override public WindowPartitionBase<Row> get() {
        return supplier.get();
    }


}
