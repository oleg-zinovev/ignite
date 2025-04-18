package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorsFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;

/**  */
final class WindowFunctionFactory<Row> implements Supplier<List<WindowFunctionWrapper<Row>>> {
    private final List<Supplier<WindowFunctionWrapper<Row>>> prototypes;
    private final RelDataType rowType;
    private final ExecutionContext<Row> ctx;

    /**  */
    WindowFunctionFactory(
        ExecutionContext<Row> ctx,
        List<AggregateCall> aggCalls,
        RelDataType inputRowType
    ) {
        rowType = inputRowType;
        this.ctx = ctx;

        ImmutableList.Builder<Supplier<WindowFunctionWrapper<Row>>> prototypes = ImmutableList.builder();
        for (AggregateCall aggCall : aggCalls) {
            if (WindowFunctions.isWindowFunction(aggCall))
                prototypes.add(new WindowFunctionWrapperPrototype(aggCall));
            else {
                prototypes.add(new WindowFunctionAccumulatorAdapterPrototype(aggCall));
            }
        }

        this.prototypes = prototypes.build();
    }

    @Override public List<WindowFunctionWrapper<Row>> get() {
        return Commons.transform(prototypes, Supplier::get);
    }

    /**  */
    private final class WindowFunctionWrapperPrototype implements Supplier<WindowFunctionWrapper<Row>> {
        /**  */
        private Supplier<WindowFunction<Row>> funcFactory;

        /**  */
        private final AggregateCall call;

        /**  */
        private Function<Row, Row> inAdapter;

        /**  */
        private Function<Object, Object> outAdapter;

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
            funcFactory = WindowFunctions.windowFunctionFactory(call);
            WindowFunction<Row> windowFunction = funcFactory.get();

            inAdapter = createInAdapter(windowFunction);
            outAdapter = createOutAdapter(windowFunction);

            return windowFunction;
        }

        /**  */
        @NotNull private Function<Row, Row> createInAdapter(WindowFunction<Row> windowFunction) {
            if (F.isEmpty(call.getArgList()))
                return Function.identity();

            List<RelDataType> inTypes = SqlTypeUtil.projectTypes(rowType, call.getArgList());
            List<RelDataType> outTypes = windowFunction.argumentTypes(ctx.getTypeFactory());

            if (call.getArgList().size() > outTypes.size()) {
                throw new AssertionError("Unexpected number of arguments: " +
                    "expected=" + outTypes.size() + ", actual=" + inTypes.size());
            }

            if (call.ignoreNulls())
                inTypes = Commons.transform(inTypes, this::nonNull);

            List<Function<Object, Object>> casts =
                Commons.transform(Pair.zip(inTypes, outTypes), AccumulatorsFactory::cast);

            final boolean ignoreNulls = call.ignoreNulls();

            final int[] argMapping = new int[Collections.max(call.getArgList()) + 1];
            Arrays.fill(argMapping, -1);

            for (int i = 0; i < call.getArgList().size(); ++i)
                argMapping[call.getArgList().get(i)] = i;

            return new Function<>() {
                final RowHandler<Row> hnd = ctx.rowHandler();

                final RowHandler.RowFactory<Row> rowFac = hnd.factory(ctx.getTypeFactory(), rowType);

                final Row reuseRow = rowFac.create();

                @Override public Row apply(Row in) {
                    Row out = reuseRow;

                    for (int i = 0; i < hnd.columnCount(in); ++i) {
                        Object val = hnd.get(i, in);

                        if (ignoreNulls && val == null)
                            return null;

                        int idx = i < argMapping.length ? argMapping[i] : -1;
                        if (idx != -1)
                            val = casts.get(idx).apply(val);

                        hnd.set(i, out, val);
                    }

                    return out;
                }
            };
        }

        /**  */
        @NotNull private Function<Object, Object> createOutAdapter(WindowFunction<Row> windowFunction) {
            RelDataType inType = windowFunction.returnType(ctx.getTypeFactory());
            RelDataType outType = call.getType();

            return AccumulatorsFactory.cast(inType, outType);
        }

        /**  */
        private RelDataType nonNull(RelDataType type) {
            return ctx.getTypeFactory().createTypeWithNullability(type, false);
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
        @Override public void add(int currRowIdx, int currPeerIdx, Row row) {
            Row accRow = inAdapter.apply(row);

            if (accRow == null)
                return;

            windowFunction.add(currRowIdx, currPeerIdx, accRow);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return outAdapter.apply(windowFunction.end());
        }
    }

    private final class WindowFunctionAccumulatorAdapterPrototype implements Supplier<WindowFunctionWrapper<Row>> {

        private final Supplier<List<AccumulatorWrapper<Row>>> factory;

        private WindowFunctionAccumulatorAdapterPrototype(AggregateCall call) {
            factory = ctx.expressionFactory().accumulatorsFactory(AggregateType.SINGLE, List.of(call), rowType);
        }

        @Override public WindowFunctionWrapper<Row> get() {
            return new WindowFunctionAccumulatorAdapter<>(factory.get().get(0));
        }
    }

    /**  */
    private static final class WindowFunctionAccumulatorAdapter<Row> implements WindowFunctionWrapper<Row> {
        private final AccumulatorWrapper<Row> acc;

        private WindowFunctionAccumulatorAdapter(AccumulatorWrapper<Row> acc) {
            this.acc = acc;
        }

        @Override public void add(int currRowIdx, int currPeerIdx, Row row) {
            acc.add(row);
        }

        @Override public Object end() {
            return acc.end();
        }
    }
}
