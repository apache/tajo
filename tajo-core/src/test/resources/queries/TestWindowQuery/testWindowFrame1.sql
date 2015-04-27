select  id,
        name,
        num,
        sum(num) over ( partition by id order by name ) as sum1,
        sum(num) over ( partition by id order by name rows 1 preceding ) as sum2,
        sum(num) over ( partition by id order by name rows between 1 preceding and 1 following) as sum3
from table1