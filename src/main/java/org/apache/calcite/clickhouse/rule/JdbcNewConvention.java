package org.apache.calcite.clickhouse.rule;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlDialect;

public class JdbcNewConvention
        extends JdbcConvention
{
    public JdbcNewConvention(SqlDialect dialect, Expression expression, String name)
    {
        super(dialect, expression, name);
    }

    @Override
    public void register(RelOptPlanner planner)
    {
        super.register(planner);
        planner.addRule(JdbcAggregateNewRule.create(this));
        planner.addRule(JdbcEnumerableProjectNewRule.create(this));
        planner.removeRule(CoreRules.AGGREGATE_CASE_TO_FILTER);
        planner.removeRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES);
        planner.removeRule(CoreRules.SORT_PROJECT_TRANSPOSE);
    }
}
