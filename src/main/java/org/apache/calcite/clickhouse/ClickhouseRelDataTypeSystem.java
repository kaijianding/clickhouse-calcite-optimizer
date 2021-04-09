package org.apache.calcite.clickhouse;

import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;

public class ClickhouseRelDataTypeSystem
        extends RelDataTypeSystemImpl
{
    public int getMaxPrecision(SqlTypeName typeName)
    {
        if (typeName == SqlTypeName.DECIMAL) {
            return 38;
        }
        return super.getMaxPrecision(typeName);
    }

    @Override
    public boolean shouldConvertRaggedUnionTypesToVarying()
    {
        return true;
    }
}
