select id, name, num,
        sum(num) over ( partition by id order by name ) as sum1,
        sum(num) over ( partition by id order by name range between current row and unbounded following) as sum2,
        sum(num) over ( partition by id order by name range between current row and current row) as sum3,
        sum(num) over ( partition by id order by name rows between unbounded preceding and current row) as sum4,
        sum(num) over ( partition by id order by name rows between current row and unbounded following) as sum5,
        sum(num) over ( partition by id order by name rows between current row and current row) as sum6
from table4
