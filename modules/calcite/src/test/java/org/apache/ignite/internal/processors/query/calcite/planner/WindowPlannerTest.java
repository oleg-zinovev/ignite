package org.apache.ignite.internal.processors.query.calcite.planner;

import java.util.List;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteWindow;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.junit.Test;

public class WindowPlannerTest extends AbstractPlannerTest {
    /**
     * @throws Exception if failed
     */
    @Test
    public void testSplitWindowByOverSpec() throws Exception {
        IgniteSchema publicSchema = createSchema(
            createTable(
                "TEST", IgniteDistributions.single(),
                "ID", Integer.class,
                "VAL", Integer.class
            ));

        String sql = "SELECT *, " +
            "SUM(1) OVER (PARTITION BY ID), " +
            "MIN(VAL) OVER (PARTITION BY ID), " +
            "MAX(2) OVER (PARTITION BY ID ORDER BY VAL ROWS BETWEEN 10 PRECEDING AND 20 FOLLOWING), " +
            "MAX(VAL) OVER (PARTITION BY ID ORDER BY VAL RANGE BETWEEN 100 PRECEDING AND 200 FOLLOWING), " +
            "ROW_NUMBER() OVER (), " +
            "DENSE_RANK() OVER (PARTITION BY ID ORDER BY VAL) " +
            "FROM TEST";

        assertPlan(sql, publicSchema,
            isInstanceOf(IgniteProject.class)
                // the top project should remove constants from the window rel on position 2..7.
                .and(project -> "[$0, $1, $8, $9, $10, $11, $12, $13]".equals(project.getProjects().toString()))
                .and(input(isInstanceOf(IgniteWindow.class)
                    .and(window -> "window(partition {0} order by [1 ASC-nulls-first] aggs [DENSE_RANK()])".equals(window.getGroup().toString()))
                    .and(window -> window.collation().equals(TraitUtils.createCollation(List.of(0, 1))))
                    .and(input(isInstanceOf(IgniteWindow.class)
                        .and(window -> "window(rows between UNBOUNDED PRECEDING and CURRENT ROW aggs [ROW_NUMBER()])".equals(window.getGroup().toString()))
                        .and(window -> window.collation().equals(TraitUtils.createCollation(List.of(0, 1))))
                        .and(input(isInstanceOf(IgniteWindow.class)
                            .and(window -> "window(partition {0} order by [1 ASC-nulls-first] range between $6 PRECEDING and $7 FOLLOWING aggs [MAX($1)])".equals(window.getGroup().toString()))
                            .and(window -> window.collation().equals(TraitUtils.createCollation(List.of(0, 1))))
                            .and(input(isInstanceOf(IgniteWindow.class)
                                .and(window -> "window(partition {0} order by [1 ASC-nulls-first] rows between $4 PRECEDING and $5 FOLLOWING aggs [MAX($3)])".equals(window.getGroup().toString()))
                                .and(window -> window.collation().equals(TraitUtils.createCollation(List.of(0, 1))))
                                .and(input(isInstanceOf(IgniteWindow.class)
                                    .and(window -> "window(partition {0} aggs [SUM($2), MIN($1)])".equals(window.getGroup().toString()))
                                    .and(window -> window.collation().equals(TraitUtils.createCollation(List.of(0, 1))))
                                    .and(input(isInstanceOf(IgniteProject.class)
                                        // the bottom project should add agg call constants from the window rel on position 2, 3
                                        .and(project -> "[$0, $1, 1, 2, 10, 20, 100, 200]".equals(project.getProjects().toString()))
                                        .and(input(isInstanceOf(IgniteSort.class)
                                            .and(sort -> sort.collation().equals(TraitUtils.createCollation(List.of(0, 1))))
                                            .and(input(isTableScan("TEST"))))))))))))))))),
            "ProjectTableScanMergeRule", "ProjectTableScanMergeSkipCorrelatedRule", "ProjectMergeRule");
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testDeriveCollation() throws Exception {
        IgniteSchema publicSchema = createSchema(
            createTable(
                "TEST", IgniteDistributions.single(),
                "ID", Integer.class,
                "VAL", Integer.class
            ));
        String sql = "SELECT MAX(VAL) OVER (PARTITION BY ID) FROM TEST ORDER BY ID DESC, VAL";

        RelCollation derivedCollation = RelCollations.of(
            TraitUtils.createFieldCollation(0, false),
            TraitUtils.createFieldCollation(1, true)
        );
        assertPlan(sql, publicSchema,
            nodeOrAnyChild(isInstanceOf(IgniteWindow.class)
                .and(window -> window.collation().equals(derivedCollation))
                .and(input(isInstanceOf(IgniteSort.class)
                    .and(sort -> sort.collation().equals(derivedCollation))))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testHashDistribution() throws Exception {
        IgniteSchema publicSchema = createSchema(
            createTable(
                "TEST", IgniteDistributions.hash(List.of(0)),
                "ID", Integer.class,
                "VAL", Integer.class
            ));
        String sql = "SELECT ROW_NUMBER() OVER (PARTITION BY ID ORDER BY VAL) FROM TEST";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteExchange.class)
                .and(hasChildThat(isInstanceOf(IgniteWindow.class))));
    }

    @Test
    public void testAffinityWithIndex() throws Exception {
        IgniteSchema publicSchema = createSchema(
            createTable(
                "TEST", IgniteDistributions.affinity(0, "test", "hash"),
                "ID", Integer.class,
                "VAL", Integer.class
            ).addIndex("TEST_IDX", 0));
        String sql = "SELECT MAX(S.VAL) OVER (PARTITION BY S.ID) FROM (SELECt * FROM TEST) S";
        assertPlan(sql, publicSchema, isInstanceOf(IgniteExchange.class)
            .and(input(hasChildThat(isInstanceOf(IgniteWindow.class)
                .and(hasDistribution(IgniteDistributions.affinity(0, "test", "hash")))
                .and(input(isIndexScan("TEST", "TEST_IDX")))))));
    }

    @Test
    public void testAffinityWithSort() throws Exception {
        IgniteSchema publicSchema = createSchema(
            createTable(
                "TEST", IgniteDistributions.affinity(0, "test", "hash"),
                "ID", Integer.class,
                "VAL", Integer.class
            ));
        String sql = "SELECT MAX(S.VAL) OVER (PARTITION BY S.ID) FROM (SELECT * FROM TEST ORDER BY ID DESC) S";
        assertPlan(sql, publicSchema, isInstanceOf(IgniteExchange.class)
            .and(input(hasChildThat(isInstanceOf(IgniteWindow.class)
                .and(hasDistribution(IgniteDistributions.affinity(0, "test", "hash")))
                .and(input(isInstanceOf(IgniteSort.class)))))));
    }
}
