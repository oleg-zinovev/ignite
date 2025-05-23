package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorsFactoryBase;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.jetbrains.annotations.NotNull;

/**  */
final class WindowFunctionFactory<Row> extends AccumulatorsFactoryBase<Row> implements Supplier<List<WindowFunctionWrapper<Row>>> {
    private final List<WindowFunctionPrototype<Row>> prototypes;
    private final RelDataType inputRowType;
    private final ExecutionContext<Row> ctx;

    /**  */
    WindowFunctionFactory(
        ExecutionContext<Row> ctx,
        Window.Group group,
        List<AggregateCall> aggCalls,
        RelDataType inputRowType
    ) {
        super(ctx);
        this.inputRowType = inputRowType;
        this.ctx = ctx;

        ImmutableList.Builder<WindowFunctionPrototype<Row>> prototypes = ImmutableList.builder();
        for (AggregateCall aggCall : aggCalls) {
            if (WindowFunctions.isWindowFunction(aggCall))
                prototypes.add(new WindowFunctionWrapperPrototype(aggCall));
            else {
                prototypes.add(new WindowFunctionAccumulatorAdapterPrototype(aggCall, group));
            }
        }

        this.prototypes = prototypes.build();
    }

    @Override public List<WindowFunctionWrapper<Row>> get() {
        return Commons.transform(prototypes, Supplier::get);
    }

    /** Checks whenether window functions require buffering */
    boolean requiresBuffering() {
        return prototypes.stream().anyMatch(WindowFunctionPrototype::requiresBuffering);
    }

    /** */
    private interface WindowFunctionPrototype<Row> extends Supplier<WindowFunctionWrapper<Row>> {
        boolean requiresBuffering();
    }

    /**  */
    private final class WindowFunctionWrapperPrototype implements WindowFunctionPrototype<Row> {
        /**  */
        private Supplier<WindowFunction<Row>> funcFactory;

        /**  */
        private final AggregateCall call;

        /**  */
        private Function<Row, Row> inAdapter;

        /**  */
        private Function<Object, Object> outAdapter;

        /** */
        private Boolean requiresBuffering;

        /**  */
        private WindowFunctionWrapperPrototype(AggregateCall call) {
            this.call = call;
        }

        /** {@inheritDoc} */
        @Override public WindowFunctionWrapper<Row> get() {
            WindowFunction<Row> windowFunction = windowFunction();
            return new WindowFunctionWrapperImpl(windowFunction, inAdapter, outAdapter);
        }

        /**  */
        @NotNull private WindowFunction<Row> windowFunction() {
            if (funcFactory != null)
                return funcFactory.get();

            // init factory and adapters
            funcFactory = WindowFunctions.windowFunctionFactory(call, ctx);
            WindowFunction<Row> windowFunction = funcFactory.get();

            inAdapter = createInAdapter(windowFunction);
            outAdapter = createOutAdapter(windowFunction);
            requiresBuffering = !(windowFunction instanceof StreamingWindowFunction);

            return windowFunction;
        }

        /**  */
        @NotNull private Function<Row, Row> createInAdapter(WindowFunction<Row> windowFunction) {
            List<RelDataType> outTypes = windowFunction.argumentTypes(ctx.getTypeFactory());
            return WindowFunctionFactory.this.createInAdapter(call, inputRowType, outTypes, false);
        }

        /**  */
        @NotNull private Function<Object, Object> createOutAdapter(WindowFunction<Row> windowFunction) {
            RelDataType inType = windowFunction.returnType(ctx.getTypeFactory());
            return WindowFunctionFactory.this.createOutAdapter(call, inType);
        }


        @Override public boolean requiresBuffering() {
            if (requiresBuffering == null) {
                Supplier<WindowFunction<Row>> tempFactory = WindowFunctions.windowFunctionFactory(call, ctx);
                requiresBuffering = !(tempFactory.get() instanceof StreamingWindowFunction);
            }

            return requiresBuffering;
        }
    }

    /**  */
    private final class WindowFunctionWrapperImpl implements WindowFunctionWrapper<Row> {
        /**  */
        private final WindowFunction<Row> windowFunction;

        /**  */
        private final Function<Row, Row> inAdapter;

        /**  */
        private final Function<Object, Object> outAdapter;

        /**  */
        WindowFunctionWrapperImpl(
            WindowFunction<Row> windowFunction,
            Function<Row, Row> inAdapter,
            Function<Object, Object> outAdapter
        ) {
            this.windowFunction = windowFunction;
            this.inAdapter = inAdapter;
            this.outAdapter = outAdapter;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row, int rowIdx, int peerIdx) {
            assert !requiresBuffering() && windowFunction instanceof StreamingWindowFunction;

            Row accRow = inAdapter.apply(row);
            if (accRow == null)
                return;

            ((StreamingWindowFunction<Row>) windowFunction).add(row, rowIdx, peerIdx);
        }

        /** {@inheritDoc} */
        @Override public void add(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
            Row accRow = inAdapter.apply(row);

            if (accRow == null)
                return;

            windowFunction.add(accRow, rowIdx, peerIdx, frame);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return outAdapter.apply(windowFunction.end());
        }
    }

    private final class WindowFunctionAccumulatorAdapterPrototype implements WindowFunctionPrototype<Row> {

        private final Supplier<AccumulatorWrapper<Row>> factory;
        private final boolean requiresBuffering;

        private WindowFunctionAccumulatorAdapterPrototype(AggregateCall call, Window.Group group) {
            Supplier<List<AccumulatorWrapper<Row>>> accFactory = ctx.expressionFactory()
                .accumulatorsFactory(AggregateType.SINGLE, List.of(call), inputRowType);
            factory = () -> accFactory.get().get(0);
            requiresBuffering = !(group.isRows
                && group.lowerBound.isUnbounded()
                && group.upperBound.isCurrentRow());
        }

        /** {@inheritDoc} */
        @Override public WindowFunctionWrapper<Row> get() {
            return new WindowFunctionAccumulatorAdapter<>(factory, requiresBuffering);
        }

        /** {@inheritDoc} */
        @Override public boolean requiresBuffering() {
            return requiresBuffering;
        }
    }

    /**  */
    private static final class WindowFunctionAccumulatorAdapter<Row> implements WindowFunctionWrapper<Row> {
        private final Supplier<AccumulatorWrapper<Row>> factory;
        private final boolean requiresBuffering;

        private AccumulatorWrapper<Row> accHolder;
        private int frameStart = -1;
        private int frameEnd = -1;

        private WindowFunctionAccumulatorAdapter(Supplier<AccumulatorWrapper<Row>> factory, boolean requiresBuffering) {
            this.factory = factory;
            this.requiresBuffering = requiresBuffering;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row, int rowIdx, int peerIdx) {
            assert !requiresBuffering;
            accumulator().add(row);
        }

        /** {@inheritDoc} */
        @Override public void add(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
            int start = frame.getFrameStart(row, rowIdx, peerIdx);
            int end = frame.getFrameEnd(row, rowIdx, peerIdx);

            if (frameStart != start || frameEnd > end) {
                // recalculate accumulator if start idx changed.
                frameStart = start;
                accHolder = null;
                for (int i = frameStart; i <= end; i++) {
                    Row valueRow = frame.get(i);
                    accumulator().add(valueRow);
                }
            } else if (frameEnd != end && end >= 0) {
                // append rows to accumulator
                for (int i = frameEnd + 1; i <= end; i++) {
                    Row valueRow = frame.get(i);
                    accumulator().add(valueRow);
                }
            }
            frameEnd = end;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return accumulator().end();
        }

        private AccumulatorWrapper<Row> accumulator() {
            if (accHolder != null)
                return accHolder;
            accHolder = factory.get();
            return accHolder;
        }
    }
}
