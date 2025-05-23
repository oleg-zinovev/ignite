package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.IgniteRexBuilder;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.window.WindowPartitionBase;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

/**
 *
 */
public class WindowExecutionTest extends AbstractExecutionTest {

    private static final int TEST_GRP_PARAM_NUM = LAST_PARAM_NUM + 1;
    private static final int TEST_AGG_PARAM_NUM = TEST_GRP_PARAM_NUM + 1;
    private static final int TEST_ARG_PARAM_NUM = TEST_AGG_PARAM_NUM + 1;
    private static final int TEST_RES_PARAM_NUM = TEST_ARG_PARAM_NUM + 1;

    /**  */
    @Parameterized.Parameter(TEST_GRP_PARAM_NUM)
    public Window.Group testGrp;

    /**  */
    @Parameterized.Parameter(TEST_AGG_PARAM_NUM)
    public List<SqlAggFunction> testAgg;

    /**  */
    @Parameterized.Parameter(TEST_ARG_PARAM_NUM)
    public List<ImmutableIntList> testArg;

    /**  */
    @Parameterized.Parameter(TEST_RES_PARAM_NUM)
    public List<List<Integer>> testRes;

    /**  */
    @Parameterized.Parameters(name = PARAMS_STRING
        + ", grp={" + TEST_GRP_PARAM_NUM
        + "}, agg={" + TEST_AGG_PARAM_NUM
        + "}, arg={" + TEST_ARG_PARAM_NUM
        + "}, res={" + TEST_RES_PARAM_NUM + "}")
    public static List<Object[]> data() {
        IgniteTypeFactory tf = new IgniteTypeFactory();
        RexBuilder rexBuilder = new IgniteRexBuilder(tf);

        List<Object[]> extraParams = new ArrayList<>();
        ImmutableList<Object[]> newParams = ImmutableList.of(
            new Object[] {
                // row_number() over (partition by {0} rows between unbounded prescending and current row)
                new Window.Group(
                    ImmutableBitSet.of(0),
                    true,
                    RexWindowBounds.UNBOUNDED_PRECEDING,
                    RexWindowBounds.CURRENT_ROW,
                    RelCollations.EMPTY,
                    ImmutableList.of()
                ),
                List.of(SqlStdOperatorTable.ROW_NUMBER),
                F.asList(ImmutableIntList.of()),
                F.asList(
                    F.asList(1),
                    F.asList(2),
                    F.asList(1),
                    F.asList(2),
                    F.asList(3),
                    F.asList(1)
                )
            },
            new Object[] {
                // row_number() over ()
                new Window.Group(
                    ImmutableBitSet.of(),
                    true,
                    RexWindowBounds.UNBOUNDED_PRECEDING,
                    RexWindowBounds.CURRENT_ROW,
                    RelCollations.EMPTY,
                    ImmutableList.of()
                ),
                List.of(SqlStdOperatorTable.ROW_NUMBER),
                F.asList(ImmutableIntList.of()),
                F.asList(
                    F.asList(1),
                    F.asList(2),
                    F.asList(3),
                    F.asList(4),
                    F.asList(5),
                    F.asList(6)
                )
            },
            new Object[] {
                // dense_rank() over (partition by {0} order by {1} range between unbounded prescending and current row)
                new Window.Group(
                    ImmutableBitSet.of(0),
                    false,
                    RexWindowBounds.UNBOUNDED_PRECEDING,
                    RexWindowBounds.CURRENT_ROW,
                    RelCollations.of(1),
                    ImmutableList.of()
                ),
                List.of(SqlStdOperatorTable.DENSE_RANK),
                F.asList(ImmutableIntList.of()),
                F.asList(
                    F.asList(1),
                    F.asList(1),
                    F.asList(1),
                    F.asList(2),
                    F.asList(3),
                    F.asList(1)
                )
            },
            new Object[] {
                // count({0}) over (partition by {0} order by {1} range between unbounded prescending and current row)
                new Window.Group(
                    ImmutableBitSet.of(0),
                    false,
                    RexWindowBounds.UNBOUNDED_PRECEDING,
                    RexWindowBounds.CURRENT_ROW,
                    RelCollations.of(1),
                    ImmutableList.of()
                ),
                List.of(SqlStdOperatorTable.COUNT),
                F.asList(ImmutableIntList.of()),
                F.asList(
                    F.asList(2),
                    F.asList(2),
                    F.asList(1),
                    F.asList(2),
                    F.asList(3),
                    F.asList(1)
                )
            },
            new Object[] {
                // count({0}) over (partition by {0} rows between unbounded prescending and unbounded following)
                new Window.Group(
                    ImmutableBitSet.of(0),
                    true,
                    RexWindowBounds.UNBOUNDED_PRECEDING,
                    RexWindowBounds.UNBOUNDED_FOLLOWING,
                    RelCollations.of(),
                    ImmutableList.of()
                ),
                List.of(SqlStdOperatorTable.COUNT),
                F.asList(ImmutableIntList.of()),
                F.asList(
                    F.asList(2),
                    F.asList(2),
                    F.asList(3),
                    F.asList(3),
                    F.asList(3),
                    F.asList(1)
                )
            },
            new Object[] {
                // count({0}) over (partition by {0} range between unbounded prescending and current row)
                new Window.Group(
                    ImmutableBitSet.of(0),
                    false,
                    RexWindowBounds.UNBOUNDED_PRECEDING,
                    RexWindowBounds.CURRENT_ROW,
                    RelCollations.EMPTY,
                    ImmutableList.of()
                ),
                List.of(SqlStdOperatorTable.COUNT),
                F.asList(ImmutableIntList.of()),
                F.asList(
                    F.asList(2),
                    F.asList(2),
                    F.asList(3),
                    F.asList(3),
                    F.asList(3),
                    F.asList(1)
                )
            },
            new Object[] {
                // count({0}) over (partition by {0} order by {2} range between 3 prescending and 1 following)
                new Window.Group(
                    ImmutableBitSet.of(0),
                    false,
                    RexWindowBounds.preceding(rexBuilder.makeLiteral(3, tf.createJavaType(int.class))),
                    RexWindowBounds.following(rexBuilder.makeLiteral(1, tf.createJavaType(int.class))),
                    RelCollations.of(2),
                    ImmutableList.of()
                ),
                List.of(SqlStdOperatorTable.COUNT),
                F.asList(ImmutableIntList.of()),
                F.asList(
                    F.asList(2),
                    F.asList(2),
                    F.asList(1),
                    F.asList(3),
                    F.asList(3),
                    F.asList(1)
                )
            },
            new Object[] {
                // count({0}) over (partition by {0} order by {1} range between {3} prescending and current row)
                new Window.Group(
                    ImmutableBitSet.of(0),
                    false,
                    RexWindowBounds.preceding(rexBuilder.makeInputRef(tf.createJavaType(int.class), 3)),
                    RexWindowBounds.CURRENT_ROW,
                    RelCollations.of(1),
                    ImmutableList.of()
                ),
                List.of(SqlStdOperatorTable.COUNT),
                F.asList(ImmutableIntList.of()),
                F.asList(
                    F.asList(2),
                    F.asList(2),
                    F.asList(1),
                    F.asList(2),
                    F.asList(1),
                    F.asList(1)
                )
            },
            new Object[] {
                // count({0}) over (partition by {0} rows between 1 prescending and 1 following)
                new Window.Group(
                    ImmutableBitSet.of(0),
                    true,
                    RexWindowBounds.preceding(rexBuilder.makeLiteral(1, tf.createJavaType(int.class))),
                    RexWindowBounds.following(rexBuilder.makeLiteral(1, tf.createJavaType(int.class))),
                    RelCollations.of(),
                    ImmutableList.of()
                ),
                List.of(SqlStdOperatorTable.COUNT),
                F.asList(ImmutableIntList.of()),
                F.asList(
                    F.asList(2),
                    F.asList(2),
                    F.asList(2),
                    F.asList(3),
                    F.asList(2),
                    F.asList(1)
                )
            },
            new Object[] {
                // row_number() over (partition by {0} rows between unbounded prescending and current row),
                // count({0}) over (partition by {0} rows between unbounded prescending and current row)
                new Window.Group(
                    ImmutableBitSet.of(0),
                    true,
                    RexWindowBounds.UNBOUNDED_PRECEDING,
                    RexWindowBounds.CURRENT_ROW,
                    RelCollations.EMPTY,
                    ImmutableList.of()
                ),
                List.of(SqlStdOperatorTable.ROW_NUMBER, SqlStdOperatorTable.SUM),
                F.asList(ImmutableIntList.of(), ImmutableIntList.of(0)),
                F.asList(
                    F.asList(1, 1),
                    F.asList(2, 2),
                    F.asList(1, 2),
                    F.asList(2, 4),
                    F.asList(3, 6),
                    F.asList(1, 3)
                )
            }
        );

        for (Object[] newParam : newParams) {
            for (Object[] inheritedParam : AbstractExecutionTest.parameters()) {
                Object[] both = Stream.concat(Arrays.stream(inheritedParam), Arrays.stream(newParam))
                    .toArray(Object[]::new);
                extraParams.add(both);
            }
        }

        return extraParams;
    }

    @Test
    public void executeWindow() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, int.class, int.class, int.class);
        Class<?>[] outFields = new Class<?>[4 + testAgg.size()];
        Arrays.fill(outFields, int.class);
        RelDataType outRowType = TypeUtils.createRowType(tf, outFields);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row(1, 1, 1, 0),
            row(1, 1, 1, 1),
            row(2, 1, 3, 0),
            row(2, 2, 5, 1),
            row(2, 3, 6, 0),
            row(3, 0, 1, 1)
        ));

        List<AggregateCall> calls = new ArrayList<>();
        for (int i = 0; i < testAgg.size(); i++) {
            SqlAggFunction op = testAgg.get(i);
            ImmutableIntList arg = testArg.get(i);
            AggregateCall call = AggregateCall.create(
                op,
                false,
                false,
                false,
                arg,
                -1,
                RelCollations.EMPTY,
                tf.createJavaType(int.class),
                null);
            calls.add(call);
        }

        WindowNode<Object[]> window = new WindowNode<>(
            ctx,
            outRowType,
            frameFactory(ctx, testGrp, calls, rowType),
            rowFactory()
        );

        window.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, outRowType);
        root.register(window);

        for (int i = 0; i < testRes.size(); i++) {
            assertTrue(root.hasNext());
            Object[] row = root.next();
            for (int j = 0; j < testRes.get(i).size(); j++) {
                assertEquals(testRes.get(i).get(j), row[4 + j]);
            }
        }
        assertFalse(root.hasNext());
    }

    /**
     * @throws Exception If failed.
     */
    @Before
    @Override public void setup() throws Exception {
        nodesCnt = 1;
        super.setup();
    }

    protected Supplier<WindowPartitionBase<Object[]>> frameFactory(
        ExecutionContext<Object[]> ctx,
        Window.Group group,
        List<AggregateCall> calls,
        RelDataType rowType
    ) {
        return ctx.expressionFactory().windowFrameFactory(group, calls, rowType, false);
    }

    /**  */
    protected RowHandler.RowFactory<Object[]> rowFactory() {
        return new RowHandler.RowFactory<Object[]>() {
            /** */
            @Override public RowHandler<Object[]> handler() {
                return ArrayRowHandler.INSTANCE;
            }

            /** */
            @Override public Object[] create() {
                throw new AssertionError();
            }

            /** */
            @Override public Object[] create(Object... fields) {
                return fields;
            }
        };
    }
}
