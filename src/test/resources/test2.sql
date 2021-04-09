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