package org.apache.ignite.internal.processors.query.calcite.rule;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteWindow;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class WindowConverterRule extends AbstractIgniteConverterRule<LogicalWindow> {
    /**
     *
     */
    public static final RelOptRule INSTANCE = new WindowConverterRule();

    /**
     *
     */
    public WindowConverterRule() {
        super(LogicalWindow.class, "WindowConverterRule");
    }

    /**
     * {@inheritDoc}
     */
    @Override protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalWindow window) {
        RelOptCluster cluster = window.getCluster();

        RelNode result = window.getInput();

        assert window.constants.isEmpty();

        for (int grpIdx = 0; grpIdx < window.groups.size(); grpIdx++) {
            Window.Group group = window.groups.get(grpIdx);

            RelCollation collation = TraitUtils.mergeCollations(
                TraitUtils.createCollation(group.keys.asList()),
                group.collation()
            );

            RelTraitSet inTraits = cluster
                .traitSetOf(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .replace(collation);

            RelTraitSet outTraits = cluster
                .traitSetOf(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .replace(collation);

            result = convert(result, inTraits);

            // add fields added by current group.
            // see org.apache.calcite.rel.logical.LogicalWindow#create
            String groupFieldPrefix = "w" + grpIdx + "$";
            List<RelDataTypeField> fieldsAddedByCurrentGroup = U.arrayList(window.getRowType().getFieldList(),
                it -> it.getName().startsWith(groupFieldPrefix));
            List<RelDataTypeField> groupFields = new ArrayList<>(result.getRowType().getFieldList());
            groupFields.addAll(fieldsAddedByCurrentGroup);

            RelRecordType rowType = new RelRecordType(groupFields);

            result = new IgniteWindow(
                window.getCluster(),
                window.getTraitSet().merge(outTraits),
                result,
                rowType,
                group
            );
        }

        return (PhysicalNode)result;
    }
}
