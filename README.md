# clickhouse-calcite-optimizer

Sometimes, clickhouse sql is not hand-written and is generated in very inefficient way, for example:

```sql
SELECT count(DISTINCT table_1611286072985.column_563569d9ca)                                          city_num,
       count(DISTINCT table_1611286072985.column_49ea7a3ef8)                                          route_num,
       count(table_1611286072985.column_71f2c1a189)                                                   type_num,
       count(DISTINCT table_1611286072985.column_e98573edeb)                                          line_num,
       count(DISTINCT table_1611286072985.column_99f7781b21)                                          datas_num,
       sum((CASE WHEN (table_1611286072985.column_71f2c1a189 = 'dynamic gps') THEN 1 ELSE 0 END))     gps_num,
       sum((CASE WHEN (table_1611286072985.column_71f2c1a189 = 'dynamic station') THEN 1 ELSE 0 END)) station_num
FROM (
         SELECT `city_id`       AS column_563569d9ca,
                `datasource_id` AS column_99f7781b21,
                `jw_data_type`  AS column_71f2c1a189,
                `line_no`       AS column_e98573edeb,
                `dt`            AS dt,
                `route_no`      AS column_49ea7a3ef8
         FROM test_db.test_table
         WHERE (dt = '2021-01-25')
           and (jw_data_type = '中文')
     ) table_1611286072985
WHERE (table_1611286072985.dt >= '2021-01-25' and table_1611286072985.dt <= '2021-02-25')
group by table_1611286072985.column_563569d9ca, table_1611286072985.column_99f7781b21
```

This sql is equals to the below one but is 10x slower

```sql
SELECT assumeNotNull(COUNT(DISTINCT `city_id`))                                         AS `city_num`,
       assumeNotNull(COUNT(DISTINCT `route_no`))                                        AS `route_num`,
       COUNT(*)                                                                         AS `type_num`,
       assumeNotNull(COUNT(DISTINCT `line_no`))                                         AS `line_num`,
       assumeNotNull(COUNT(DISTINCT `datasource_id`))                                   AS `datas_num`,
       COALESCE(SUM(CASE WHEN `jw_data_type` = 'dynamic gps' THEN 1 ELSE 0 END), 0)     AS `gps_num`,
       COALESCE(SUM(CASE WHEN `jw_data_type` = 'dynamic station' THEN 1 ELSE 0 END), 0) AS `station_num`
FROM `test_db`.`test_table`
WHERE `dt` = '2021-01-25'
  AND `jw_data_type` = '中文'
  AND (`dt` >= '2021-01-25' AND `dt` <= '2021-02-25')
GROUP BY `city_id`, `datasource_id`
```

This tool use calcite to do the optimization and transform the sql to more efficient way  

## Usage
```java
String sql = ClickhouseSqlOptimizer.INSTANCE.optimize(
    new ClickhouseConnectionSpec(
        "jdbc:clickhouse://" + TEST_HOST + "/" + TEST_DB,
        TEST_USER,
        TEST_PASSWORD,
        "ru.yandex.clickhouse.ClickHouseDriver",
        TEST_DB
    ),
    input_sql
)
```