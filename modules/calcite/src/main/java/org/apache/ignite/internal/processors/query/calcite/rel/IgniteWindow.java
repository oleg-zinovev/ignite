package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.externalize.RelInputEx;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitsAwareIgniteRel;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;

/**
 * A relational expression representing a set of window aggregates.
 *
 * <p>A Window can handle several window aggregate functions, over several
 * partitions, with pre- and post-expressions, and an optional post-filter.
 * Each of the partitions is defined by a partition key (zero or more columns)
 * and a range (logical or physical). The partitions expect the data to be
 * sorted correctly on input to the relational expression.
 *
 * <p>Each {@link Window.Group} has a set of
 * {@link org.apache.calcite.rex.RexOver} objects.
 */
public class IgniteWindow extends Window implements TraitsAwareIgniteRel {

    private final Group group;

    public IgniteWindow(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
        RelDataType rowType, Group group) {
        super(cluster, traitSet, input, ImmutableList.of(), rowType, ImmutableList.of(group));
        this.group = group;
    }

    public IgniteWindow(RelInput input) {
        this(input.getCluster(),
            changeTraits(input, IgniteConvention.INSTANCE).getTraitSet(),
            input.getInput(),
            input.getRowType("rowType"),
            ((RelInputEx)input).getWindowGroup("group"));
    }

    /**  */
    public Group getGroup() {
        return group;
    }

    /** {@inheritDoc} */
    @Override public Window copy(List<RexLiteral> constants) {
        assert constants.isEmpty();
        return this;
    }

    /** {@inheritDoc} */
    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IgniteWindow(getCluster(), traitSet, sole(inputs), getRowType(), group);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("rowType", getRowType())
            .item("group", group);
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory)planner.getCostFactory();

        double rowCnt = mq.getRowCount(getInput());

        return costFactory.makeCost(rowCnt,
            rowCnt * IgniteCost.ROW_COMPARISON_COST,
            0,
            rowCnt * IgniteCost.AGG_CALL_MEM_COST,
            0
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteWindow(cluster, getTraitSet(), sole(inputs), getRowType(), group);
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        // The node is rewindable if its input is rewindable.

        RelTraitSet in = inTraits.get(0);
        RewindabilityTrait rewindability = TraitUtils.rewindability(in);

        return ImmutableList.of(Pair.of(nodeTraits.replace(rewindability), ImmutableList.of(in)));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughDistribution(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        if (TraitUtils.distribution(nodeTraits).satisfies(IgniteDistributions.single()))
            return null;
        else
            return TraitsAwareIgniteRel.super.passThroughDistribution(nodeTraits, inTraits);
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        RelTraitSet in = inTraits.get(0);

        if (TraitUtils.distribution(in).satisfies(IgniteDistributions.single()))
            return ImmutableList.of();

        return ImmutableList.of(Pair.of(nodeTraits.replace(TraitUtils.distribution(in)), ImmutableList.of(in)));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {

        RelCollation inputCollation = TraitUtils.collation(inTraits.get(0));

        if (!inputCollation.satisfies(collation()))
            return ImmutableList.of();

        return ImmutableList.of(Pair.of(nodeTraits.replace(inputCollation), inTraits));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCorrelation(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        return ImmutableList.of(Pair.of(nodeTraits.replace(TraitUtils.correlation(inTraits.get(0))), inTraits));
    }
}
