select * from (
select
  sum(l_orderkey) + sum(l_partkey) as total
from
  lineitem
group by
  l_orderkey + l_partkey
) t1
order by
  total;