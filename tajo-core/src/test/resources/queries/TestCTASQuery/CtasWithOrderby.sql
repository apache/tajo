create table testCtasWithOrderby (col1 float, col2 float) partition by column(key float8) as
select
  sum(l_orderkey) as total1,
  avg(l_partkey) as total2,
  l_quantity as key
from
  lineitem
group by
  l_quantity
order by
  l_quantity;