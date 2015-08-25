create table testCtasWithTextFile (col1 float, col2 float) using text as
select
  sum(l_orderkey) as total1,
  avg(l_partkey) as total2
from
  lineitem
group by
  l_quantity
order by
  l_quantity
limit
  3;