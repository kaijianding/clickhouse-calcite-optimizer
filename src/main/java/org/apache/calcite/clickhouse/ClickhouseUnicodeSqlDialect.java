package org.apache.calcite.clickhouse;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.ClickHouseSqlDialect;
import org.apache.calcite.util.RelToSqlConverterUtil;

public class ClickhouseUnicodeSqlDialect
        extends ClickHouseSqlDialect
{
    public static final SqlDialect INSTANCE = new ClickhouseUnicodeSqlDialect();

    public ClickhouseUnicodeSqlDialect()
    {
        super(DEFAULT_CONTEXT);
    }

    /**
     * Appends a string literal to a buffer.
     *
     * @param buf         Buffer
     * @param charsetName Character set name, e.g. "utf16", or null
     * @param val         String value
     */
    public void quoteStringLiteral(StringBuilder buf, String charsetName, String val)
    {
        if (containsNonAscii(val) && charsetName == null) {
            quoteStringLiteralUnicode(buf, val);
        }
        else {
            if (charsetName != null) {
                buf.append("_");
                buf.append(charsetName);
            }
            buf.append(literalQuoteString);
            buf.append(val.replace(literalEndQuoteString, literalEscapedQuote));
            buf.append(literalEndQuoteString);
        }
    }

    @Override
    public void quoteStringLiteralUnicode(StringBuilder buf, String val)
    {
        buf.append("'");
        buf.append(val);
        buf.append("'");
    }

    @Override
    public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec)
    {
        if (ClickhouseSqlOperatorTable.INSTANCE.isCustomizedAggregators(call.getOperator().getName())) {
            RelToSqlConverterUtil.specialOperatorByName(call.getOperator().getName())
                    .unparse(writer, call, 0, 0);
            return;
        }
        super.unparseCall(writer, call, leftPrec, rightPrec);
    }
}
