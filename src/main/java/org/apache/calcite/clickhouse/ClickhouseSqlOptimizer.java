package org.apache.calcite.clickhouse;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.ConversionUtil;

import java.nio.charset.Charset;
import java.util.Properties;
import java.util.function.UnaryOperator;

public class ClickhouseSqlOptimizer
{
    public static final ClickhouseSqlOptimizer INSTANCE = new ClickhouseSqlOptimizer();

    private static final Charset DEFAULT_CHARSET = Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);

    static {
        System.setProperty("saffron.default.charset", DEFAULT_CHARSET.name());
        System.setProperty("saffron.default.nationalcharset", DEFAULT_CHARSET.name());
        System.setProperty("saffron.default.collation.name", ConversionUtil.NATIVE_UTF16_CHARSET_NAME + "$en_US");
    }

    private static final SqlDialect DIALECT = ClickhouseUnicodeSqlDialect.INSTANCE;

    private static final SqlParser.Config PARSER_CONFIG = SqlParser
            .config()
            .withCaseSensitive(false)
            .withUnquotedCasing(Casing.UNCHANGED)
            .withQuotedCasing(Casing.UNCHANGED)
            .withQuoting(Quoting.BACK_TICK)
            .withConformance(SqlConformanceEnum.MYSQL_5);

    private static final SqlValidator.Config VALIDATE_CONFIG = SqlValidator.Config.DEFAULT
            .withSqlConformance(SqlConformanceEnum.MYSQL_5)
            .withDefaultNullCollation(NullCollation.HIGH)
            .withIdentifierExpansion(true);

    private static final SqlToRelConverter.Config SQL_TO_REL_CONFIG = SqlToRelConverter
            .config()
            .withDecorrelationEnabled(false)
            .withTrimUnusedFields(true)
            .withInSubQueryThreshold(Integer.MAX_VALUE);

    private final Context context;

    public ClickhouseSqlOptimizer()
    {
        Properties info = new Properties();
        info.put("lenientOperatorLookup", "true");
        info.put("TYPE_SYSTEM", "org.apache.calcite.clickhouse.ClickhouseRelDataTypeSystem");
        context = Contexts.of(new CalciteConnectionConfigImpl(info));
    }

    public String optimize(ClickhouseConnectionSpec spec, String sql)
            throws Exception
    {
        final Planner planner = createPlanner(spec.getSchema());
        SqlNode parse = planner.parse(sql);
        SqlNode validate = planner.validate(parse);
        RelNode rel = planner.rel(validate).rel;

        RelTraitSet desiredTraits = rel.getTraitSet().replace(EnumerableConvention.INSTANCE);
        rel = planner.transform(0, desiredTraits, rel);
        return toSql(rel);
    }

    private Planner createPlanner(SchemaPlus schema)
    {
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(PARSER_CONFIG)
                .sqlValidatorConfig(VALIDATE_CONFIG)
                .context(context)
                .sqlToRelConverterConfig(SQL_TO_REL_CONFIG)
                .programs(Programs.standard())
                .operatorTable(ClickhouseSqlOperatorTable.INSTANCE)
                .defaultSchema(schema)
                .build();
        return Frameworks.getPlanner(config);
    }

    private String toSql(RelNode root)
    {
        return toSql(root, DIALECT, c ->
                c.withAlwaysUseParentheses(false)
                        .withSelectListItemsOnSeparateLines(true)
                        .withIndentation(2));
    }

    /**
     * Converts a relational expression to SQL in a given dialect
     * and with a particular writer configuration.
     */
    private String toSql(RelNode root, SqlDialect dialect,
            UnaryOperator<SqlWriterConfig> transform)
    {
        final JdbcImplementor converter = new JdbcImplementor(dialect, (JavaTypeFactory) root.getCluster().getTypeFactory());
        return converter.visitInput(root, 0).asStatement().toSqlString(c -> transform.apply(c.withDialect(dialect))).getSql();
    }
}
