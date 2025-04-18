package org.apache.ignite.internal.processors.query.calcite.planner;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteLimit;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

public class WindowPlannerTest extends AbstractPlannerTest {

    /** Row count in table. */
    private static final double ROW_CNT = 100d;

    /**
     * @throws Exception if failed
     */
    @Test
    public void testWindow() throws Exception {
        IgniteSchema publicSchema = createSchemaWithTable(IgniteDistributions.broadcast());

        String sql = "SELECT ID, SUM(1) OVER (PARTITION BY ID), AVG(VAL) OVER (PARTITION BY ID), MAX(VAL) OVER () FROM TEST";
        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteLimit.class))
            .and(hasChildThat(isInstanceOf(IgniteSort.class)).negate()));
    }

    /**
     * Creates PUBLIC schema with one TEST table.
     */
    private IgniteSchema createSchemaWithTable(IgniteDistribution distr, int... indexedColumns) {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        RelDataType type = new RelDataTypeFactory.Builder(f)
            .add("ID", f.createJavaType(Integer.class))
            .add("VAL", f.createJavaType(String.class))
            .build();

        TestTable table = new TestTable("TEST", type, ROW_CNT) {
            @Override public IgniteDistribution distribution() {
                return distr;
            }
        };

        if (!F.isEmpty(indexedColumns))
            table.addIndex("test_idx", indexedColumns);

        return createSchema(table);
    }
}
