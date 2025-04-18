package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

/** */
class WindowFunctions {

    /** */
    static boolean isWindowFunction(AggregateCall call) {
        String name = call.getAggregation().getName();
        return "ROW_NUMBER".equals(name) || "DENSE_RANK".equals(name);
    }

    /** */
    static <Row> Supplier<WindowFunction<Row>> windowFunctionFactory(AggregateCall call) {
        switch (call.getAggregation().getName()) {
            case "ROW_NUMBER":
                return RowNumber::new;
            case "DENSE_RANK":
                return DenseRank::new;
            default:
                throw new AssertionError(call.getAggregation().getName());
        }
    }

    /**
     * ROW_NUMBER window function implementation.
     */
    private static class RowNumber<Row> implements WindowFunction<Row> {

        private int result = - 1;

        @Override public void add(int currRowIdx, int currPeerIdx, Row row) {
            result = currRowIdx;
        }

        @Override public Object end() {
            return result == -1 ? null : result + 1;
        }

        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of();
        }

        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(INTEGER);
        }
    }

    /**
     * DENSE_RANK window function implementation.
     */
    private static class DenseRank<Row> implements WindowFunction<Row> {

        private int result = - 1;

        @Override public void add(int currRowIdx, int currPeerIdx, Row row) {
            result = currPeerIdx;
        }

        @Override public Object end() {
            return result == -1 ? null : result + 1;
        }

        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of();
        }

        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(INTEGER);
        }
    }
}
