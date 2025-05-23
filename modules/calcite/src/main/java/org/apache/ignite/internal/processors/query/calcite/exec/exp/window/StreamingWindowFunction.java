package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

/** Interface for a window function supporting streaming. */
interface StreamingWindowFunction<Row> extends WindowFunction<Row> {

    /** */
    void add(Row row, int rowIdx, int peerIdx);

    /** {@inheritDoc} */
    @Override default void add(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
        add(row, rowIdx, peerIdx);
    }
}
