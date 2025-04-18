package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.Arrays;
import java.util.Comparator;
import java.util.UUID;
import java.util.function.IntFunction;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.IgniteRexBuilder;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.window.Framing;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class WindowTest extends AbstractExecutionTest {

    /**  */
    @Test
    public void rowNumber() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class);
        RelDataType outRowType = TypeUtils.createRowType(tf, int.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row(1),
            row(1),
            row(2),
            row(2),
            row(2),
            row(3)
        ));

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.ROW_NUMBER,
            false,
            false,
            false,
            ImmutableIntList.of(),
            -1,
            RelCollations.EMPTY,
            tf.createJavaType(int.class),
            null);
        Window.Group group = new Window.Group(
            ImmutableBitSet.of(0),
            true,
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            RelCollations.EMPTY,
            ImmutableList.of()
        );

        WindowNode<Object[]> window = new WindowNode<>(
            ctx,
            outRowType,
            grpComp(ctx, group),
            frameFactory(ctx, group, call, rowType),
            rowFactory()
        );

        window.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, outRowType);
        root.register(window);

        assertTrue(root.hasNext());
        assertEquals(1, root.next()[1]);
        assertTrue(root.hasNext());
        assertEquals(2, root.next()[1]);
        assertTrue(root.hasNext());
        assertEquals(1, root.next()[1]);
        assertTrue(root.hasNext());
        assertEquals(2, root.next()[1]);
        assertTrue(root.hasNext());
        assertEquals(3, root.next()[1]);
        assertTrue(root.hasNext());
        assertEquals(1, root.next()[1]);
        assertFalse(root.hasNext());
    }

    /**  */
    @Test
    public void countROWS() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class);
        RelDataType outRowType = TypeUtils.createRowType(tf, int.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row(1),
            row(1),
            row(2),
            row(3)
        ));

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableIntList.of(),
            -1,
            RelCollations.EMPTY,
            tf.createJavaType(int.class),
            null);
        Window.Group group = new Window.Group(
            ImmutableBitSet.of(0),
            true,
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            RelCollations.EMPTY,
            ImmutableList.of()
        );

        WindowNode<Object[]> window = new WindowNode<>(
            ctx,
            outRowType,
            grpComp(ctx, group),
            frameFactory(ctx, group, call, rowType),
            rowFactory()
        );

        window.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, outRowType);
        root.register(window);

        assertTrue(root.hasNext());
        assertEquals(1, root.next()[1]);
        assertTrue(root.hasNext());
        assertEquals(2, root.next()[1]);
        assertTrue(root.hasNext());
        assertEquals(1, root.next()[1]);
        assertTrue(root.hasNext());
        assertEquals(1, root.next()[1]);
        assertFalse(root.hasNext());
    }

    /**  */
    @Test
    public void denseRank() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, int.class);
        RelDataType outRowType = TypeUtils.createRowType(tf, int.class, int.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row(1, 1),
            row(1, 1),
            row(1, 2),
            row(1, 2),
            row(1, 2),
            row(2, 1)
        ));

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.DENSE_RANK,
            false,
            false,
            false,
            ImmutableIntList.of(),
            -1,
            RelCollations.EMPTY,
            tf.createJavaType(int.class),
            null);
        Window.Group group = new Window.Group(
            ImmutableBitSet.of(0),
            false,
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            RelCollations.of(1),
            ImmutableList.of()
        );

        WindowNode<Object[]> window = new WindowNode<>(
            ctx,
            outRowType,
            grpComp(ctx, group),
            frameFactory(ctx, group, call, rowType),
            rowFactory()
        );

        window.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, outRowType);
        root.register(window);

        assertTrue(root.hasNext());
        assertEquals(1, root.next()[2]);
        assertTrue(root.hasNext());
        assertEquals(1, root.next()[2]);
        assertTrue(root.hasNext());
        assertEquals(2, root.next()[2]);
        assertTrue(root.hasNext());
        assertEquals(2, root.next()[2]);
        assertTrue(root.hasNext());
        assertEquals(2, root.next()[2]);
        assertTrue(root.hasNext());
        assertEquals(1, root.next()[2]);
        assertFalse(root.hasNext());
    }

    /**  */
    @Test
    public void countRANGE() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, int.class);
        RelDataType outRowType = TypeUtils.createRowType(tf, int.class, int.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row(1, 1),
            row(1, 1),
            row(1, 2),
            row(2, 1)
        ));

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableIntList.of(),
            -1,
            RelCollations.EMPTY,
            tf.createJavaType(int.class),
            null);
        Window.Group group = new Window.Group(
            ImmutableBitSet.of(0),
            false,
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            RelCollations.of(1),
            ImmutableList.of()
        );

        WindowNode<Object[]> window = new WindowNode<>(
            ctx,
            outRowType,
            grpComp(ctx, group),
            frameFactory(ctx, group, call, rowType),
            rowFactory()
        );

        window.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, outRowType);
        root.register(window);

        assertTrue(root.hasNext());
        assertEquals(2, root.next()[2]);
        assertTrue(root.hasNext());
        assertEquals(2, root.next()[2]);
        assertTrue(root.hasNext());
        assertEquals(3, root.next()[2]);
        assertTrue(root.hasNext());
        assertEquals(1, root.next()[2]);
        assertFalse(root.hasNext());
    }

    /**  */
    @Test
    public void countRANGE_WO_PEER() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, int.class);
        RelDataType outRowType = TypeUtils.createRowType(tf, int.class, int.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row(1, 1),
            row(1, 1),
            row(1, 2),
            row(2, 1)
        ));

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableIntList.of(),
            -1,
            RelCollations.EMPTY,
            tf.createJavaType(int.class),
            null);
        Window.Group group = new Window.Group(
            ImmutableBitSet.of(0),
            false,
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            RelCollations.EMPTY,
            ImmutableList.of()
        );

        WindowNode<Object[]> window = new WindowNode<>(
            ctx,
            outRowType,
            grpComp(ctx, group),
            frameFactory(ctx, group, call, rowType),
            rowFactory()
        );

        window.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, outRowType);
        root.register(window);

        assertTrue(root.hasNext());
        assertEquals(3, root.next()[2]);
        assertTrue(root.hasNext());
        assertEquals(3, root.next()[2]);
        assertTrue(root.hasNext());
        assertEquals(3, root.next()[2]);
        assertTrue(root.hasNext());
        assertEquals(1, root.next()[2]);
        assertFalse(root.hasNext());
    }

    /**  */
    @Test
    public void countRANGE_BOUNDS() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        IgniteTypeFactory tf = ctx.getTypeFactory();
        RexBuilder rexBuilder = new IgniteRexBuilder(tf);

        RelDataType rowType = TypeUtils.createRowType(tf, int.class, int.class);
        RelDataType outRowType = TypeUtils.createRowType(tf, int.class, int.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row(1, 1),
            row(1, 1),
            row(1, 3),
            row(1, 3),
            row(1, 4),
            row(2, 1)
        ));

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableIntList.of(),
            -1,
            RelCollations.EMPTY,
            tf.createJavaType(int.class),
            null);
        Window.Group group = new Window.Group(
            ImmutableBitSet.of(0),
            false,
            RexWindowBounds.preceding(rexBuilder.makeLiteral(2, tf.createJavaType(int.class))),
            RexWindowBounds.following(rexBuilder.makeLiteral(1, tf.createJavaType(int.class))),
            RelCollations.of(1),
            ImmutableList.of()
        );

        WindowNode<Object[]> window = new WindowNode<>(
            ctx,
            outRowType,
            grpComp(ctx, group),
            frameFactory(ctx, group, call, rowType),
            rowFactory()
        );

        window.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, outRowType);
        root.register(window);

        assertTrue(root.hasNext());
        assertEquals(2, root.next()[2]);
        assertTrue(root.hasNext());
        assertEquals(2, root.next()[2]);
        assertTrue(root.hasNext());
        assertEquals(5, root.next()[2]);
        assertTrue(root.hasNext());
        assertEquals(5, root.next()[2]);
        assertTrue(root.hasNext());
        assertEquals(3, root.next()[2]);
        assertTrue(root.hasNext());
        assertEquals(1, root.next()[2]);
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

    /**  */
    protected Comparator<Object[]> grpComp(
        ExecutionContext<Object[]> ctx,
        Window.Group group
    ) {
        RelCollation collation = TraitUtils.createCollation(group.keys.asList());
        return ctx.expressionFactory().comparator(collation);
    }

    /**  */
    protected IntFunction<Framing<Object[]>> frameFactory(
        ExecutionContext<Object[]> ctx,
        Window.Group group,
        AggregateCall call,
        RelDataType rowType
    ) {
        return ctx.expressionFactory().windowFrameFactory(group, F.asList(call), rowType);
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
