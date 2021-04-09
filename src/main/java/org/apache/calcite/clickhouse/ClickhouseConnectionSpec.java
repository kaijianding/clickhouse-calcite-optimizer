package org.apache.calcite.clickhouse;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.clickhouse.rule.JdbcNewConvention;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.tools.Frameworks;

import javax.sql.DataSource;

public class ClickhouseConnectionSpec
{
    private final SchemaPlus schema;

    public ClickhouseConnectionSpec(String url, String username, String password, String driver, String schema)
    {
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        DataSource dataSource = JdbcSchema.dataSource(url, driver, username, password);
        this.schema = rootSchema.add(schema, create(rootSchema, schema, dataSource, schema));
    }

    private JdbcSchema create(
            SchemaPlus parentSchema,
            String name,
            DataSource dataSource,
            String schema
    )
    {
        final Expression expression =
                Schemas.subSchemaExpression(parentSchema, name, JdbcSchema.class);
        final JdbcConvention convention =
                new JdbcNewConvention(ClickhouseUnicodeSqlDialect.INSTANCE, expression, name);
        return new JdbcSchema(dataSource, ClickhouseUnicodeSqlDialect.INSTANCE, convention, null, schema);
    }

    public SchemaPlus getSchema()
    {
        return schema;
    }
}
