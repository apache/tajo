select
  sum(l_orderkey) as total1 , sum(l_orderkey) as total2
from
  lineitem
group by
  l_orderkey + l_partkey
order by
  total1, total2;