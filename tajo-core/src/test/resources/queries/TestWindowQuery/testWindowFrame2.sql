select id, name, num,
      first_value(num) over ( partition by id order by name rows between unbounded preceding and current row ) as first_value_rows,
      first_value(num) over ( partition by id order by name range between unbounded preceding and current row ) as first_value_range,
      last_value(num) over ( partition by id order by name rows between unbounded preceding and current row ) as last_value_rows,
      last_value(num) over ( partition by id order by name range between unbounded preceding and current row ) as last_value_range
from table2
