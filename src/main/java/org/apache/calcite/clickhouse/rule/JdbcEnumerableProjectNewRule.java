package org.apache.calcite.clickhouse.rule;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Project;

public class JdbcEnumerableProjectNewRule
        extends ConverterRule
{
    public static JdbcEnumerableProjectNewRule create(JdbcConvention out)
    {
        return Config.INSTANCE
                .withConversion(Project.class, project ->
                                (out.dialect.supportsWindowFunctions()
                                        || !project.containsOver()),
                        EnumerableConvention.INSTANCE, out, "JdbcEnumerableProjectNewRule")
                .withRuleFactory(JdbcEnumerableProjectNewRule::new)
                .toRule(JdbcEnumerableProjectNewRule.class);
    }

    /**
     * Creates an JdbcEnumerableProjectRule.
     */
    protected JdbcEnumerableProjectNewRule(Config config)
    {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel)
    {
        final Project project = (Project) rel;

        return new JdbcRules.JdbcProject(
                rel.getCluster(),
                rel.getTraitSet().replace(out),
                convert(
                        project.getInput(),
                        project.getInput().getTraitSet().replace(out)),
                project.getProjects(),
                project.getRowType());
    }
}
