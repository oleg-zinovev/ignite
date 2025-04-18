package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.IgniteRexBuilder;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * Factory to create {@link Framing} factory from {@link Window.Group}
 */
public final class FramingFactory<Row> implements IntFunction<Framing<Row>> {

    private final IntFunction<Framing<Row>> supplier;

    public FramingFactory(
        ExecutionContext<Row> ctx,
        Window.Group group,
        Supplier<List<WindowAccumulator<Row>>> accFactory,
        RelDataType inputRowType
    ) {
        IntFunction<Framing<Row>> supplier;
        if (group.isRows) {
            int lowerOffset = boundToOffset(ctx, group.lowerBound);
            int upperOffset = boundToOffset(ctx, group.upperBound);
            supplier = (size) -> new RowFraming<>(accFactory, lowerOffset, upperOffset, size);
        }
        else {
            Comparator<Row> peedCmp = ctx.expressionFactory().comparator(group.collation());
            Function<Row, Row> lowerOffset = boundToProject(ctx, group.lowerBound, group.collation(), inputRowType);
            Function<Row, Row> upperOffset = boundToProject(ctx, group.upperBound, group.collation(), inputRowType);
            supplier = (size) -> new RangeFraming<>(accFactory, peedCmp, lowerOffset, upperOffset, size);
        }

        this.supplier = supplier;
    }

    @Override public Framing<Row> apply(int bufferSize) {
        return supplier.apply(bufferSize);
    }

    private int boundToOffset(ExecutionContext<Row> ctx, RexWindowBound bound) {
        int result;
        if (bound.isCurrentRow()) {
            result = 0;
        }
        else if (bound.isUnbounded()) {
            result = Integer.MAX_VALUE;
        }
        else {
            assert bound.getOffset() != null : "Unexpected null offset in bounded window";
            result = ctx.expressionFactory().<Integer>execute(bound.getOffset()).get();
        }

        assert result >= 0 : "Unexpected negative offset in window";

        if (bound.isPreceding()) {
            result = -result;
        }

        return result;
    }

    private Function<Row, Row> boundToProject(ExecutionContext<Row> ctx, RexWindowBound bound, RelCollation collation, RelDataType rowType) {
        if (bound.isCurrentRow()) {
            return Function.identity();
        }
        else if (bound.isUnbounded()) {
            return row -> null;
        }
        else {
            assert bound.getOffset() != null : "Unexpected null offset in bounded window";
            assert collation.getFieldCollations().size() == 1 : "Unexpected number of field collations in bounded window";

            RelFieldCollation field = collation.getFieldCollations().get(0);
            int fieldIdx = field.getFieldIndex();
            List<RelDataTypeField> fields = rowType.getFieldList();
            assert fieldIdx < rowType.getFieldList().size();
            RelDataType fieldType = fields.get(fieldIdx).getType();

            SqlOperator operator = getBoundOperator(bound, field);

            IgniteTypeFactory typeFactory = Commons.typeFactory();
            RexBuilder builder = new IgniteRexBuilder(typeFactory);

            List<RexNode> project = new ArrayList<>(rowType.getFieldCount());
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                project.add(builder.makeInputRef(fields.get(i).getType(), i));
            }


            RexNode offset = builder.makeCast(fieldType, bound.getOffset());

            RexNode call = builder.makeCall(operator, ImmutableList.of(project.get(fieldIdx), offset));

            call = builder.makeCast(fieldType, call);

            project.set(fieldIdx, call);

            return ctx.expressionFactory().project(project, rowType);
        }
    }

    private static SqlOperator getBoundOperator(RexWindowBound bound, RelFieldCollation field) {
        if (field.direction.isDescending()) {
            if (bound.isPreceding()) {
                return SqlStdOperatorTable.PLUS;
            }
            else {
                return SqlStdOperatorTable.MINUS;
            }
        }
        else {
            if (bound.isPreceding()) {
                return SqlStdOperatorTable.MINUS;
            }
            else {
                return SqlStdOperatorTable.PLUS;
            }
        }
    }
}
