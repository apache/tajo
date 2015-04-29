select id, name, num,
        sum(num) over ( partition by id order by name rows between 1 preceding and 2 following) as sum1,
        sum(num) over ( partition by id order by name rows between 2 preceding and 1 preceding) as sum2,
        sum(num) over ( partition by id order by name rows between 1 following and 2 following) as sum3
from table6