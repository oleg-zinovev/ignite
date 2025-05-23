package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.List;
import java.util.Set;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.processors.query.calcite.externalize.RelInputEx;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

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
public abstract class IgniteWindowBase extends Window implements IgniteRel {

    private final Group group;

    protected IgniteWindowBase(RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RelDataType rowType,
        Group group
    ) {
        super(cluster, traitSet, input, ImmutableList.of(), rowType, ImmutableList.of(group));
        this.group = group;
        assert !group.aggCalls.isEmpty();
    }

    protected IgniteWindowBase(RelInput input) {
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
    @Override public RelWriter explainTerms(RelWriter pw) {
        return pw
            .input("input", getInput())
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
            estimateMemory(mq),
            0
        );
    }

    /** Estimates memory cost */
    protected abstract double estimateMemory(RelMetadataQuery mq);

    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        if (required.getConvention() != IgniteConvention.INSTANCE)
            return null;

        RelTraitSet traits = required;
        RelCollation requiredCollation = TraitUtils.collation(required);
        if (!satisfiesCollationSansGroupFields(requiredCollation)) {
            traits = traits.replace(collation());
        }

        IgniteDistribution distribution = TraitUtils.distribution(required);
        if (!satisfiesDistribution(distribution))
            traits = traits.replace(distribution());
        else if (distribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
            // Group set contains all distribution keys, shift distribution keys according to used columns.
            IgniteDistribution outDistribution = distribution.apply(Commons.mapping(group.keys, rowType.getFieldCount()));
            traits = traits.replace(outDistribution);
        }

        if (traits == traitSet) {
            // cannot pass through or derive traits.
            return null;
        }

        return Pair.of(traits, ImmutableList.of(traits));
    }

    @Override public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        assert childId == 0;

        return passThroughTraits(childTraits);
    }

    /**
     * Check input distribution satisfies collation of this window.
     */
    private boolean satisfiesDistribution(IgniteDistribution distribution) {
        if (distribution.satisfies(IgniteDistributions.single()) || distribution.function().correlated()) {
            return true;
        }

        if (distribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
            for (Integer key : distribution.getKeys()) {
                if (!group.keys.get(key))
                    // can't derive distribution with fields unmatched to group keys
                    return false;
            }
            return true;
        }

        return false;
    }

    /**
     * Check input collation satisfies collation of this window.
     * - Collations field indicies of the window should be a prefix for desired collation.
     * - Group fields sort direction can be changed to desired collation.
     * - Order fields sort direction should be the same as in desired collation.
     */
    private boolean satisfiesCollationSansGroupFields(RelCollation desiredCollation) {
        RelCollation collation = collation();
        if (desiredCollation.satisfies(collation)) {
            return true;
        }

        if (!Util.startsWith(desiredCollation.getKeys(), collation.getKeys())) {
            return false;
        }

        int grpKeysSize = group.keys.cardinality();
        // strip group keys
        List<RelFieldCollation> desiredFieldCollations = Util.skip(desiredCollation.getFieldCollations(), grpKeysSize);
        List<RelFieldCollation> fieldCollations = Util.skip(collation.getFieldCollations(), grpKeysSize);
        return Util.startsWith(desiredFieldCollations, fieldCollations);
    }

    /**
     * Check group requires buffering the whole partition.
     * todo: move to utility class?
     */
    public static boolean requiresBuffering(Window.Group group) {
        // Can execute window streaming in case:

        // group aggs does not contain operators can access whole partition.
        if (group.aggCalls.stream().anyMatch(it -> BUFFERING_FUNCTIONS.contains(it.op))) {
            return true;
        }

        // group aggs contains only ROW_NUMBER, RANK, DENSE_RANK operators
        if (group.aggCalls.stream().allMatch(it -> STREAMING_FUNCTIONS.contains(it.op))) {
            return false;
        }

        // group frame in 'ROWS BETWEEN UNBOUNDED PRESCENDING AND CURRENT ROW'
        //noinspection RedundantIfStatement
        if (group.isRows && group.lowerBound.isUnbounded() && group.upperBound.isCurrentRow()) {
            return false;
        }

        return true;
    }

    // Window functions, which definitly supports sreaming execution.
    private static final Set<SqlOperator> STREAMING_FUNCTIONS = Set.of(
        SqlStdOperatorTable.ROW_NUMBER,
        SqlStdOperatorTable.RANK,
        SqlStdOperatorTable.DENSE_RANK
    );

    // Window functions, which definitly requires buffering.
    private static final Set<SqlOperator> BUFFERING_FUNCTIONS = Set.of(
        SqlStdOperatorTable.PERCENT_RANK,
        SqlStdOperatorTable.CUME_DIST,
        SqlStdOperatorTable.FIRST_VALUE,
        SqlStdOperatorTable.LAST_VALUE,
        SqlStdOperatorTable.LAG,
        SqlStdOperatorTable.LEAD
    );

}
