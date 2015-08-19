select
  l_orderkey as gkey,
  count(1) as unique_key
from
  lineitem
group by
  lineitem.l_orderkey
order by
  gkey;