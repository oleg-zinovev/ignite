package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

/** interface for window function wrapper. */
interface WindowFunctionWrapper<Row> {
    /** */
    void add(Row row, int rowIdx, int peerIdx);

    /** */
    void add(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame);

    /** */
    Object end();
}
