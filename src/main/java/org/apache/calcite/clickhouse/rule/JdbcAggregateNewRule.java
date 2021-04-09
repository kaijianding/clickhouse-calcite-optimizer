package org.apache.calcite.clickhouse.rule;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

public class JdbcAggregateNewRule extends JdbcRules.JdbcAggregateRule
{
    protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

    public static JdbcAggregateNewRule create(JdbcConvention out) {
        return Config.INSTANCE
                .withConversion(Aggregate.class, Convention.NONE, out,
                        "JdbcAggregateNewRule")
                .withRuleFactory(JdbcAggregateNewRule::new)
                .toRule(JdbcAggregateNewRule.class);
    }

    /** Called from the Config. */
    protected JdbcAggregateNewRule(Config config) {
        super(config);
    }

    @Override public RelNode convert(RelNode rel) {
        final Aggregate agg = (Aggregate) rel;
        if (agg.getGroupSets().size() != 1) {
            // GROUPING SETS not supported; see
            // [CALCITE-734] Push GROUPING SETS to underlying SQL via JDBC adapter
            return null;
        }
        final RelTraitSet traitSet =
                agg.getTraitSet().replace(out);
        try {
            return new JdbcNewAggregate(rel.getCluster(), traitSet,
                    convert(agg.getInput(), out), agg.getGroupSet(),
                    agg.getGroupSets(), agg.getAggCallList());
        } catch (InvalidRelException e) {
            LOGGER.debug(e.toString());
            return null;
        }
    }
}
