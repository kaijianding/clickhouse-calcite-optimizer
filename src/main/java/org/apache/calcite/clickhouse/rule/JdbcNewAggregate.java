package org.apache.calcite.clickhouse.rule;

import org.apache.calcite.adapter.jdbc.JdbcRel;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Aggregate operator implemented in JDBC convention.
 */
public class JdbcNewAggregate
        extends JdbcRules.JdbcAggregate
        implements JdbcRel
{
    public JdbcNewAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls)
            throws InvalidRelException
    {
        super(cluster, traitSet, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public JdbcNewAggregate copy(RelTraitSet traitSet, RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls)
    {
        try {
            return new JdbcNewAggregate(getCluster(), traitSet, input,
                    groupSet, groupSets, aggCalls);
        }
        catch (InvalidRelException e) {
            // Semantic error not possible. Must be a bug. Convert to
            // internal error.
            throw new AssertionError(e);
        }
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
    {
        RelOptCost cost = super.computeSelfCost(planner, mq);
        if (cost != null) {
            return cost.multiplyBy(0.1);
        }
        return null;
    }
}
