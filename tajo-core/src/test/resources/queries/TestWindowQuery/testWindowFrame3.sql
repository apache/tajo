select id, name, num,
        first_value(num) over ( partition by id order by name rows between current row and unbounded following) as first_value_rows,
        first_value(num) over ( partition by id order by name range between current row and unbounded following) as first_value_range,
        last_value(num) over ( partition by id order by name rows between current row and unbounded following) as last_value_rows,
        last_value(num) over ( partition by id order by name range between current row and unbounded following) as last_value_range
from table3
