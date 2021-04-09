(SELECT city_name
 from test_db.test_table
 where city_name = 'aa' limit 1)

union all

SELECT city_name
from test_db.test_table2