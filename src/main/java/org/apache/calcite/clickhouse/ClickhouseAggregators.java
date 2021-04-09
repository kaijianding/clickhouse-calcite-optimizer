package org.apache.calcite.clickhouse;

import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import java.util.HashMap;
import java.util.Map;

public class ClickhouseAggregators
{
    public static final Map<String, SqlReturnTypeInference> PREDEFINED_AGGREGATORS = new HashMap<>();

    static {
        PREDEFINED_AGGREGATORS.put("uniq", ReturnTypes.BIGINT);
        PREDEFINED_AGGREGATORS.put("uniqExact", ReturnTypes.BIGINT);
        PREDEFINED_AGGREGATORS.put("uniqHLL12", ReturnTypes.BIGINT);
        PREDEFINED_AGGREGATORS.put("uniqCombined", ReturnTypes.BIGINT);
        PREDEFINED_AGGREGATORS.put("uniqCombined64", ReturnTypes.BIGINT);
        PREDEFINED_AGGREGATORS.put("any", ReturnTypes.DOUBLE);
        PREDEFINED_AGGREGATORS.put("anyHeavy", ReturnTypes.DOUBLE);
        PREDEFINED_AGGREGATORS.put("anyLast", ReturnTypes.DOUBLE);
        PREDEFINED_AGGREGATORS.put("groupBitAnd", ReturnTypes.BIGINT);
        PREDEFINED_AGGREGATORS.put("groupBitOr", ReturnTypes.BIGINT);
        PREDEFINED_AGGREGATORS.put("groupBitXor", ReturnTypes.BIGINT);
        PREDEFINED_AGGREGATORS.put("groupBitmap", ReturnTypes.BIGINT);
        PREDEFINED_AGGREGATORS.put("groupBitmapAnd", ReturnTypes.BIGINT);
        PREDEFINED_AGGREGATORS.put("groupBitmapOr", ReturnTypes.BIGINT);
        PREDEFINED_AGGREGATORS.put("groupBitmapXor", ReturnTypes.BIGINT);
        PREDEFINED_AGGREGATORS.put("argMin", ReturnTypes.DOUBLE);
        PREDEFINED_AGGREGATORS.put("argMax", ReturnTypes.DOUBLE);
        PREDEFINED_AGGREGATORS.put("quantile", ReturnTypes.DOUBLE);
    }
}
