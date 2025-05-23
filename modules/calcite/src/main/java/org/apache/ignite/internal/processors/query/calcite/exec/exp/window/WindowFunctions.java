package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.List;
import java.util.function.Supplier;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlRankFunction;
import org.apache.calcite.sql.fun.SqlFirstLastValueAggFunction;
import org.apache.calcite.sql.fun.SqlLeadLagAggFunction;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

/**  */
class WindowFunctions {

    /**  */
    static boolean isWindowFunction(AggregateCall call) {
        return call.getAggregation() instanceof SqlRankFunction
            || call.getAggregation() instanceof SqlLeadLagAggFunction
            || call.getAggregation() instanceof SqlFirstLastValueAggFunction;
    }

    /**  */
    static <Row> Supplier<WindowFunction<Row>> windowFunctionFactory(
        AggregateCall call,
        ExecutionContext<Row> ctx
    ) {
        RowHandler<Row> hnd = ctx.rowHandler();
        switch (call.getAggregation().getName()) {
            case "ROW_NUMBER":
                return () -> new RowNumber<>(hnd, call);
            case "RANK":
                return () -> new Rank<>(hnd, call);
            case "DENSE_RANK":
                return () -> new DenseRank<>(hnd, call);
            case "PERCENT_RANK":
                return () -> new PercentRank<>(hnd, call);
            case "CUME_DIST":
                return () -> new CumeDist<>(hnd, call);
            case "LAG":
                return () -> new Lag<>(hnd, call);
            case "LEAD":
                return () -> new Lead<>(hnd, call);
            case "FIRST_VALUE":
                return () -> new FirstValue<>(hnd, call);
            case "LAST_VALUE":
                return () -> new LastValue<>(hnd, call);
            default:
                throw new AssertionError(call.getAggregation().getName());
        }
    }

    /**  */
    private abstract static class AbstractWindowFunction<Row> {
        /**  */
        private final RowHandler<Row> hnd;
        /**  */
        private final AggregateCall aggCall;

        private AbstractWindowFunction(RowHandler<Row> hnd, AggregateCall aggCall) {
            this.hnd = hnd;
            this.aggCall = aggCall;
        }

        /**  */
        <T> T get(int idx, Row row) {
            assert idx < arguments().size() : "idx=" + idx + "; arguments=" + arguments();

            return (T)hnd.get(arguments().get(idx), row);
        }

        /**  */
        protected AggregateCall aggregateCall() {
            return aggCall;
        }

        /**  */
        protected List<Integer> arguments() {
            return aggCall.getArgList();
        }

        /**  */
        int columnCount(Row row) {
            return hnd.columnCount(row);
        }
    }

    /**  */
    private abstract static class AbstractLagLeadWindowFunction<Row> extends AbstractWindowFunction<Row> implements WindowFunction<Row> {
        private AbstractLagLeadWindowFunction(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        protected int getOffset(Row row) {
            if (arguments().size() > 1)
                return get(1, row);
            else
                return 1;
        }

        protected Object getDefault(Row row) {
            if (arguments().size() > 2)
                return get(2, row);
            else
                return null;
        }

        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            int argSize = arguments().size();
            assert argSize >= 1 && argSize <= 3 : "Unexpected arguments count: " + argSize;

            ImmutableList.Builder<RelDataType> builder = ImmutableList.builderWithExpectedSize(argSize);
            builder.add(typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true));
            if (argSize > 1) {
                builder.add(typeFactory.createTypeWithNullability(typeFactory.createSqlType(INTEGER), false));
            }
            if (argSize > 2) {
                builder.add(typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true));
            }
            return builder.build();
        }

        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true);
        }
    }

    /**
     * ROW_NUMBER window function implementation.
     */
    private static class RowNumber<Row> extends AbstractWindowFunction<Row> implements StreamingWindowFunction<Row> {

        private long rowNumber = -1;

        private RowNumber(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        @Override public void add(Row row, int rowIdx, int peerIdx) {
            rowNumber = rowIdx;
        }

        @Override public Object end() {
            return rowNumber + 1;
        }

        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of();
        }

        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(BIGINT);
        }
    }

    /**
     * RANK window function implementation.
     */
    private static class Rank<Row> extends AbstractWindowFunction<Row> implements StreamingWindowFunction<Row> {

        private int previousPeerIdx = -1;
        private long rank = 1;
        private long count;

        private Rank(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        @Override public void add(Row row, int rowIdx, int peerIdx) {
            if (previousPeerIdx != peerIdx) {
                previousPeerIdx = peerIdx;
                rank += count;
                count = 0;
            }
            count++;
        }

        @Override public Object end() {
            return rank;
        }

        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of();
        }

        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(BIGINT);
        }
    }

    /**
     * DENSE_RANK window function implementation.
     */
    private static class DenseRank<Row> extends AbstractWindowFunction<Row> implements StreamingWindowFunction<Row> {

        private long denseRank;

        private DenseRank(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        @Override public void add(Row row, int rowIdx, int peerIdx) {
            denseRank = peerIdx;
        }

        @Override public Object end() {
            return denseRank + 1;
        }

        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of();
        }

        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(BIGINT);
        }
    }

    /**
     * PERCENT_RANK window function implementation.
     */
    private static class PercentRank<Row> extends AbstractWindowFunction<Row> implements WindowFunction<Row> {

        private int previousPeerIdx = -1;
        private long rank = 1;
        private long count;
        private double percentRank;

        private PercentRank(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        @Override public void add(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
            if (previousPeerIdx != peerIdx) {
                previousPeerIdx = peerIdx;
                rank += count;
                count = 0;
            }
            count++;
            int size = frame.partitionSize() - 1;
            if (size == 0)
                percentRank = 0.0;
            else {
                percentRank = ((double)rank - 1) / size;
            }
        }

        @Override public Object end() {
            return percentRank;
        }

        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of();
        }

        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(DOUBLE);
        }
    }

    /**
     * CUME_DIST window function implementation.
     */
    private static class CumeDist<Row> extends AbstractWindowFunction<Row> implements WindowFunction<Row> {

        private long count;
        private double cumeDist;

        private CumeDist(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        @Override public void add(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
            count = frame.size(rowIdx, peerIdx);
            cumeDist = ((double)count) / frame.partitionSize();
        }

        @Override public Object end() {
            return cumeDist;
        }

        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of();
        }

        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(DOUBLE);
        }
    }

    /**
     * LAG window function implementation.
     */
    private static class Lag<Row> extends AbstractLagLeadWindowFunction<Row> {

        private Object value;

        private Lag(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        @Override public void add(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
            int offset = getOffset(row);
            int idx = rowIdx - offset;
            if (idx < 0 || idx >= frame.partitionSize())
                value = getDefault(row);
            else {
                Row lagRow = frame.get(idx);
                value = get(0, lagRow);
                if (value == null) {
                    value = getDefault(row);
                }
            }
        }

        @Override public Object end() {
            return value;
        }
    }

    /**
     * LEAD window function implementation.
     */
    private static class Lead<Row> extends AbstractLagLeadWindowFunction<Row> {

        private Object value;

        private Lead(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        @Override public void add(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
            int offset = getOffset(row);
            int idx = rowIdx + offset;
            if (idx < 0 || idx >= frame.partitionSize())
                value = getDefault(row);
            else {
                Row leadRow = frame.get(idx);
                value = get(0, leadRow);
                if (value == null) {
                    value = getDefault(row);
                }
            }
        }

        @Override public Object end() {
            return value;
        }
    }

    /**
     * FIRST_VALUE window function implementation.
     */
    private static class FirstValue<Row> extends AbstractWindowFunction<Row> implements WindowFunction<Row> {

        private Object value;

        private FirstValue(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        @Override public void add(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
            int startIdx = frame.getFrameStart(row, rowIdx, peerIdx);
            Row firstRow = frame.get(startIdx);
            value = get(0, firstRow);
        }

        @Override public Object end() {
            return value;
        }

        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true));
        }

        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true);
        }
    }

    /**
     * LAST_VALUE window function implementation.
     */
    private static class LastValue<Row> extends AbstractWindowFunction<Row> implements WindowFunction<Row> {
        private Object value;

        private LastValue(RowHandler<Row> hnd, AggregateCall aggCall) {
            super(hnd, aggCall);
        }

        @Override public void add(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
            int endIdx = frame.getFrameEnd(row, rowIdx, peerIdx);
            if (endIdx < 0)
                value = null;
            else {
                Row lastRow = frame.get(endIdx);
                value = get(0, lastRow);
            }
        }

        @Override public Object end() {
            return value;
        }

        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true));
        }

        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true);
        }
    }
}
