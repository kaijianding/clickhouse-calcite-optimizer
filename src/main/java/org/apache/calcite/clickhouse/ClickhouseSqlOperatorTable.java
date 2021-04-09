package org.apache.calcite.clickhouse;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.util.Optionality;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.calcite.clickhouse.ClickhouseAggregators.PREDEFINED_AGGREGATORS;

public class ClickhouseSqlOperatorTable
        extends ChainedSqlOperatorTable
{
    public static final ClickhouseSqlOperatorTable INSTANCE = new ClickhouseSqlOperatorTable();

    private final ListSqlOperatorTable listOpTab;
    private final Set<String> customizedAggregators = new HashSet<>();

    public ClickhouseSqlOperatorTable()
    {
        super(ImmutableList.of(SqlStdOperatorTable.instance(), new ListSqlOperatorTable()));
        this.listOpTab = (ListSqlOperatorTable) this.tableList.get(1);
        for (Map.Entry<String, SqlReturnTypeInference> entry : PREDEFINED_AGGREGATORS.entrySet()) {
            addOperator(entry.getKey(), entry.getValue());
        }
    }

    public boolean isCustomizedAggregators(String op)
    {
        return customizedAggregators.contains(op);
    }

    public void addOperator(String op, SqlReturnTypeInference returnTypeInference)
    {
        if (!customizedAggregators.contains(op)) {
            listOpTab.add(new ClickhouseAggregator(op, returnTypeInference));
            customizedAggregators.add(op);
        }
    }

    private static class ClickhouseAggregator
            extends SqlAggFunction
    {
        public ClickhouseAggregator(String name, SqlReturnTypeInference returnTypeInference)
        {
            super(
                    name,
                    null,
                    SqlKind.MAX,
                    returnTypeInference,
                    null,
                    OperandTypes.ONE_OR_MORE,
                    SqlFunctionCategory.USER_DEFINED_FUNCTION,
                    false,
                    false,
                    Optionality.FORBIDDEN
            );
        }
    }
}
