package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

/** */
public interface WindowAccumulator<Row> {
    /** */
    void add(int currRowIdx, int currPeerIdx, int rowIdx, int peerIdx, Row row);

    /** */
    Object end();
}
