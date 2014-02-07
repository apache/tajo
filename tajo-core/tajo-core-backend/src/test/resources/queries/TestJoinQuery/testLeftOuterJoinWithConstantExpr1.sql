select
  c_custkey,
  orders.o_orderkey,
  'val' as val
from
  customer left outer join orders on c_custkey = o_orderkey
order by
  c_custkey,
  o_orderkey;