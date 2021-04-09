SELECT count(*)
FROM (
         SELECT 1
         FROM test_db.test_table
         WHERE dt = '2021-01-01'
           AND leader_name = 'b'
           AND lead_city_name = 'a'
     ) tmp_0