package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

/** interface for window function wrapper. */
interface WindowFunctionWrapper<Row> {
    /** */
    void add(int currRowIdx, int currPeerIdx, Row row);

    /** */
    Object end();
}
