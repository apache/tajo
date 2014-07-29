select
  l_orderkey,
  avg(l_partkey) total,
  sum(l_linenumber) as num
from
  lineitem
group by
  l_orderkey
having
  total >= 2 or num = 3
order by
  l_orderkey,
  total;