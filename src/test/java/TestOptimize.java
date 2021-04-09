import org.apache.calcite.clickhouse.ClickhouseConnectionSpec;
import org.apache.calcite.clickhouse.ClickhouseSqlOptimizer;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class TestOptimize
{
    private static final String TEST_HOST = "the_clickhouse_host:8023";
    private static final String TEST_USER = "default";
    private static final String TEST_PASSWORD = "xxxx";
    private static final String TEST_DB = "test_db";

    @Test
    public void test()
            throws Exception
    {
        String file = "test.sql";
        String sql = new String(Files.readAllBytes(Paths.get(TestOptimize.class.getClassLoader().getResource(file).getPath())));
        sql = ClickhouseSqlOptimizer.INSTANCE.optimize(
                new ClickhouseConnectionSpec(
                        "jdbc:clickhouse://" + TEST_HOST + "/" + TEST_DB,
                        TEST_USER,
                        TEST_PASSWORD,
                        "ru.yandex.clickhouse.ClickHouseDriver",
                        TEST_DB
                ),
                sql
        );
        System.out.println(sql);
    }
}
