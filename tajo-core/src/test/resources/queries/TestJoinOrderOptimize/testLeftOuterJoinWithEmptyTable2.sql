explain global select
  c_custkey,
  sum(empty_orders.o_orderkey),
  max(empty_orders.o_orderstatus),
  max(empty_orders.o_orderdate)
from
  customer left outer join empty_orders on c_custkey = o_orderkey
  group by c_custkey
order by c_custkey ;