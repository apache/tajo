select
  sum(l_orderkey) + sum(l_orderkey) as total
from
  lineitem
group by
  l_orderkey + l_partkey
order by
  l_orderkey + l_partkey;