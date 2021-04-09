SELECT a.city_name
FROM (
         SELECT city_name
         from test_db.test_table
     ) a
         LEFT JOIN
     (
         SELECT city_name
         FROM test_db.test_table2
     ) b on a.city_name = b.city_name
where a.city_name = 'aa'