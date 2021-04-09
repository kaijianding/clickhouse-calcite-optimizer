SELECT sum((CASE WHEN (city_id = 1) THEN 1 ELSE 0 END)), uniq(city_id)
FROM test_db.test_table