package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
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

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

/**
 * Factory to create {@link Framing} factory from {@link Window.Group}
 */
public final class FramingFactory<Row> implements IntFunction<Framing<Row>> {

    private final IntFunction<Framing<Row>> supplier;

    public FramingFactory(
        ExecutionContext<Row> ctx,
        Window.Group group,
        List<AggregateCall> calls,
        RelDataType inputRowType
    ) {
        WindowFunctionFactory<Row> accFactory = new WindowFunctionFactory<>(ctx, calls, inputRowType);

        IntFunction<Framing<Row>> supplier;
        if (group.isRows) {
            Function<Row, Integer> lowerOffset = rowsBoundToOffset(ctx, group.lowerBound, inputRowType);
            Function<Row, Integer> upperOffset = rowsBoundToOffset(ctx, group.upperBound, inputRowType);
            supplier = (size) -> new RowFraming<>(accFactory, lowerOffset, upperOffset, size);
        }
        else {
            Comparator<Row> peerCmp = ctx.expressionFactory().comparator(group.collation());
            Function<Row, Row> lowerOffset = rangeBoundToProject(ctx, group.lowerBound, group.collation(), inputRowType);
            Function<Row, Row> upperOffset = rangeBoundToProject(ctx, group.upperBound, group.collation(), inputRowType);
            supplier = (size) -> new RangeFraming<>(accFactory, peerCmp, lowerOffset, upperOffset, size);
        }

        this.supplier = supplier;
    }

    @Override public Framing<Row> apply(int bufferSize) {
        return supplier.apply(bufferSize);
    }

    private Function<Row, Integer> rowsBoundToOffset(
        ExecutionContext<Row> ctx,
        RexWindowBound bound,
        RelDataType rowType
    ) {
        if (bound.isCurrentRow())
            return ignored -> 0;
        else if (bound.isUnbounded() && bound.isPreceding())
            return ignored -> null;
        else {
            assert bound.getOffset() != null : "Unexpected null offset in bounded window";

            IgniteTypeFactory typeFactory = Commons.typeFactory();
            RexBuilder builder = new IgniteRexBuilder(typeFactory);
            RexNode result = builder.makeCast(typeFactory.createSqlType(INTEGER), bound.getOffset());
            Function<Row, Row> project = ctx.expressionFactory().project(List.of(result), rowType);
            return project.andThen(row -> (Integer)ctx.rowHandler().get(0, row));
        }
    }

    private Function<Row, Row> rangeBoundToProject(
        ExecutionContext<Row> ctx,
        RexWindowBound bound,
        RelCollation collation,
        RelDataType rowType
    ) {
        if (bound.isCurrentRow())
            return Function.identity();
        else if (bound.isUnbounded())
            return row -> null;
        else {
            assert bound.getOffset() != null : "Unexpected null offset in bounded window";
            assert collation.getFieldCollations().size() == 1 : "Unexpected number of field collations in bounded window";

            RelFieldCollation field = collation.getFieldCollations().get(0);
            int fieldIdx = field.getFieldIndex();
            List<RelDataTypeField> fields = rowType.getFieldList();

            RelDataType fieldType = fields.get(fieldIdx).getType();

            SqlOperator operator = getRangeBoundOperator(bound, field);

            IgniteTypeFactory typeFactory = Commons.typeFactory();
            RexBuilder builder = new IgniteRexBuilder(typeFactory);

            List<RexNode> project = new ArrayList<>(rowType.getFieldCount());
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                project.add(builder.makeInputRef(fields.get(i).getType(), i));
            }

            RexNode call = builder.makeCall(operator, ImmutableList.of(project.get(fieldIdx), bound.getOffset()));

            call = builder.makeCast(fieldType, call);

            project.set(fieldIdx, call);

            return ctx.expressionFactory().project(project, rowType);
        }
    }

    private static SqlOperator getRangeBoundOperator(RexWindowBound bound, RelFieldCollation field) {
        SqlOperator operator;
        if (bound.isPreceding())
            operator = SqlStdOperatorTable.MINUS;
        else
            operator = SqlStdOperatorTable.PLUS;
        if (field.direction.isDescending()) {
            operator = INVERTED_OPERATOR.get(operator);
        }
        return operator;
    }

    private static final Map<SqlOperator, SqlOperator> INVERTED_OPERATOR = Map.of(
        SqlStdOperatorTable.PLUS, SqlStdOperatorTable.MINUS,
        SqlStdOperatorTable.MINUS, SqlStdOperatorTable.PLUS
    );
}
