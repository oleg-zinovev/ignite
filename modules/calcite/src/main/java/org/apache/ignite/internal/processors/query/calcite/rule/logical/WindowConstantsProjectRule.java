package org.apache.ignite.internal.processors.query.calcite.rule.logical;

import java.util.ArrayList;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

/**
 * A rule to split window rel with constants to:
 * - project with constants
 * - window without constants
 * - project removing constants
 */
@Value.Enclosing
public class WindowConstantsProjectRule extends RelRule<WindowConstantsProjectRule.Config> implements TransformationRule {
    /**  */
    public static final WindowConstantsProjectRule INSTANCE = new WindowConstantsProjectRule(WindowConstantsProjectRule.Config.DEFAULT);

    private WindowConstantsProjectRule(Config cfg) {
        super(cfg);
    }

    @Override public void onMatch(RelOptRuleCall call) {
        LogicalWindow window = call.rel(0);

        assert !window.constants.isEmpty();

        RelNode projectWithConstants = buildProjectWithConstants(window);
        RelNode newWindow = buildWindowWithoutConstants(window, projectWithConstants);
        RelNode projectWithoutConstants = buildProjectExcludeConstants(window, newWindow);

        assert window.getRowType().equalsSansFieldNames(projectWithoutConstants.getRowType());
        call.transformTo(projectWithoutConstants);
    }

    private RelNode buildProjectWithConstants(LogicalWindow window) {
        RelNode input = window.getInput();
        List<RelDataTypeField> inputFields = input.getRowType().getFieldList();
        int inputFieldsCount = inputFields.size();

        List<RexNode> projects = new ArrayList<>();
        for (int i = 0; i < inputFieldsCount; i++) {
            RexInputRef ref = new RexInputRef(i, inputFields.get(i).getType());
            projects.add(ref);
        }
        projects.addAll(window.constants);

        RelBuilder relBldr = relBuilderFactory.create(window.getCluster(), null);
        relBldr.push(input);
        relBldr.project(projects);
        return relBldr.build();
    }

    private RelNode buildWindowWithoutConstants(LogicalWindow window, RelNode constantsProjection) {
        int originalInputFieldCount = window.getInput().getRowType().getFieldCount();
        List<RelDataTypeField> newInputFields = constantsProjection.getRowType().getFieldList();
        assert originalInputFieldCount < newInputFields.size();

        List<RelDataTypeField> windowFields = window.getRowType().getFieldList();

        List<RelDataType> types = new ArrayList<>(windowFields.size() + window.constants.size());
        List<String> names = new ArrayList<>(windowFields.size() + window.constants.size());
        for (int i = 0; i < originalInputFieldCount; i++) {
            // add fields from original input, passed through window rel
            RelDataTypeField field = windowFields.get(i);
            types.add(field.getType());
            names.add(field.getName());
        }
        for (int i = originalInputFieldCount; i < newInputFields.size(); i++) {
            // add constants from new input with contants
            RelDataTypeField field = newInputFields.get(i);
            types.add(field.getType());
            names.add(field.getName());
        }
        for (int i = originalInputFieldCount; i < windowFields.size(); i++) {
            // add fields, provided by window
            RelDataTypeField field = windowFields.get(i);
            types.add(field.getType());
            names.add(field.getName());
        }

        RelDataTypeFactory typeFactory = window.getCluster().getTypeFactory();
        RelDataType newWindowRelType = typeFactory.createStructType(types, names);

        // agg calls in window allready projects constants as ref input with index below input fields
        // do not need to remap it
        return LogicalWindow.create(window.getTraitSet(), constantsProjection, ImmutableList.of(), newWindowRelType, window.groups);
    }

    private RelNode buildProjectExcludeConstants(LogicalWindow originalWindow, RelNode newWindow) {
        int originalInputFieldCount = originalWindow.getInput().getRowType().getFieldCount();

        List<RelDataTypeField> newWindowFields = newWindow.getRowType().getFieldList();

        List<RexNode> projects = new ArrayList<>();
        for (int i = 0; i < originalInputFieldCount; i++) {
            RexInputRef ref = new RexInputRef(i, newWindowFields.get(i).getType());
            projects.add(ref);
        }
        for (int i = originalInputFieldCount + originalWindow.constants.size(); i < newWindowFields.size(); i++) {
            RexInputRef ref = new RexInputRef(i, newWindowFields.get(i).getType());
            projects.add(ref);
        }

        RelBuilder relBldr = relBuilderFactory.create(newWindow.getCluster(), null);
        relBldr.push(newWindow);
        relBldr.project(projects);
        return relBldr.build();
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /**  */
        WindowConstantsProjectRule.Config DEFAULT = ImmutableWindowConstantsProjectRule.Config.of()
            .withOperandSupplier(b -> b.operand(LogicalWindow.class)
                .predicate(it -> !it.constants.isEmpty())
                .anyInputs());

        /** {@inheritDoc} */
        @Override default WindowConstantsProjectRule toRule() {
            return INSTANCE;
        }
    }
}
