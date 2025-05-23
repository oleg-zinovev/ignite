package org.apache.ignite.internal.processors.query.calcite.rel.window;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteWindowBase;

import static org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost.AGG_CALL_MEM_COST;

/**
 * Window implementation that buffers all rows withing partiion before processing.
 */
public class IgniteBufferingWindow extends IgniteWindowBase {
    /** */
    public IgniteBufferingWindow(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode input, RelDataType rowType, Group group) {
        super(cluster, traitSet, input, rowType, group);
    }

    /** {@inheritDoc} */
    public IgniteBufferingWindow(RelInput input) {
        super(input);
    }

    /** {@inheritDoc} */
    @Override protected double estimateMemory(RelMetadataQuery mq) {
        double inRows = mq.getRowCount(getInput());
        return inRows * getGroup().aggCalls.size() * AGG_CALL_MEM_COST;
    }

    /** {@inheritDoc} */
    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IgniteBufferingWindow(getCluster(), traitSet, sole(inputs), getRowType(), getGroup());
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteBufferingWindow(cluster, getTraitSet(), sole(inputs), getRowType(), getGroup());
    }
}
