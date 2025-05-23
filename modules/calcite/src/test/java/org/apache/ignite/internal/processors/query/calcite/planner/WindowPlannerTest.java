package org.apache.ignite.internal.processors.query.calcite.planner;

import java.util.List;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.window.IgniteBufferingWindow;
import org.apache.ignite.internal.processors.query.calcite.rel.window.IgniteStreamingWindow;
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
                // the top project should remove constants from the window rel on position 2, 3.
                .and(project -> "[$0, $1, $4, $5, $6, $7, $8, $9]".equals(project.getProjects().toString()))
                .and(input(isInstanceOf(IgniteStreamingWindow.class)
                    .and(window -> "window(partition {0} order by [1 ASC-nulls-first] aggs [DENSE_RANK()])".equals(window.getGroup().toString()))
                    .and(window -> window.collation().equals(TraitUtils.createCollation(List.of(0, 1))))
                    .and(input(isInstanceOf(IgniteStreamingWindow.class)
                        .and(window -> "window(rows between UNBOUNDED PRECEDING and CURRENT ROW aggs [ROW_NUMBER()])".equals(window.getGroup().toString()))
                        .and(window -> window.collation().equals(TraitUtils.createCollation(List.of(0, 1))))
                        .and(input(isInstanceOf(IgniteBufferingWindow.class)
                            .and(window -> "window(partition {0} order by [1 ASC-nulls-first] range between 100 PRECEDING and 200 FOLLOWING aggs [MAX($1)])".equals(window.getGroup().toString()))
                            .and(window -> window.collation().equals(TraitUtils.createCollation(List.of(0, 1))))
                            .and(input(isInstanceOf(IgniteBufferingWindow.class)
                                .and(window -> "window(partition {0} order by [1 ASC-nulls-first] rows between 10 PRECEDING and 20 FOLLOWING aggs [MAX($3)])".equals(window.getGroup().toString()))
                                .and(window -> window.collation().equals(TraitUtils.createCollation(List.of(0, 1))))
                                .and(input(isInstanceOf(IgniteBufferingWindow.class)
                                    .and(window -> "window(partition {0} aggs [SUM($2), MIN($1)])".equals(window.getGroup().toString()))
                                    .and(window -> window.collation().equals(TraitUtils.createCollation(List.of(0, 1))))
                                    .and(input(isInstanceOf(IgniteProject.class)
                                        // the bottom project should add agg call constants from the window rel on position 2, 3
                                        .and(project -> "[$0, $1, 1, 2]".equals(project.getProjects().toString()))
                                        .and(input(isInstanceOf(IgniteSort.class)
                                            .and(sort -> sort.collation().equals(TraitUtils.createCollation(List.of(0, 1))))
                                            .and(input(isTableScan("TEST"))))))))))))))))),
            "ProjectTableScanMergeRule", "ProjectTableScanMergeSkipCorrelatedRule");
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
            nodeOrAnyChild(isInstanceOf(IgniteBufferingWindow.class)
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
                .and(hasChildThat(isInstanceOf(IgniteStreamingWindow.class))));
    }

    /**
     * @throws Exception if failed
     */
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
            .and(input(hasChildThat(isInstanceOf(IgniteBufferingWindow.class)
                .and(hasDistribution(IgniteDistributions.affinity(0, "test", "hash")))
                .and(input(isIndexScan("TEST", "TEST_IDX")))))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testAffinityWithSort() throws Exception {
        IgniteSchema publicSchema = createSchema(
            createTable(
                "TEST", IgniteDistributions.affinity(0, "test", "hash"),
                "ID", Integer.class,
                "VAL", Integer.class
            ));
        String sql = "SELECT MAX(S.VAL) OVER (PARTITION BY S.ID) FROM TEST S";
        assertPlan(sql, publicSchema, isInstanceOf(IgniteExchange.class)
            .and(input(hasChildThat(isInstanceOf(IgniteBufferingWindow.class)
                .and(hasDistribution(IgniteDistributions.affinity(0, "test", "hash")))
                .and(input(isInstanceOf(IgniteSort.class)))))));
    }
}
