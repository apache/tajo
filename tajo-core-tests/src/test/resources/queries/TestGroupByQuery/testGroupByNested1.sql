select
  l_orderkey + l_partkey as unique_key
from
  lineitem
group by
  l_orderkey + l_partkey
order by
  unique_key;