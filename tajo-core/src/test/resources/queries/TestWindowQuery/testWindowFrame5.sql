select id, name, num,
        first_value(num) over ( partition by id order by name rows between 1 preceding and 2 following) as first_value_1,
        first_value(num) over ( partition by id order by name rows between 2 preceding and 1 preceding) as first_value_2,
        first_value(num) over ( partition by id order by name rows between 1 following and 2 following) as first_value_3,
        last_value(num) over ( partition by id order by name rows between 1 preceding and 2 following) as last_value_1,
        last_value(num) over ( partition by id order by name rows between 2 preceding and 1 preceding) as last_value_2,
        last_value(num) over ( partition by id order by name rows between 1 following and 2 following) as last_value_3
from table5