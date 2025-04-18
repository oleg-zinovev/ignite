package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

/** interface for window function. */
interface WindowFunction<Row> {

    /** */
    void add(int currRowIdx, int currPeerIdx, Row row);

    /** */
    Object end();

    /** */
    List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory);

    /** */
    RelDataType returnType(IgniteTypeFactory typeFactory);
}
