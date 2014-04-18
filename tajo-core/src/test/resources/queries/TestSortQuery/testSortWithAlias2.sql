select
  lineitem.l_orderkey as l_orderkey,
  count(l_partkey) as cnt
from
  lineitem a
group by
  lineitem.l_orderkey
order by
  lineitem.l_orderkey;