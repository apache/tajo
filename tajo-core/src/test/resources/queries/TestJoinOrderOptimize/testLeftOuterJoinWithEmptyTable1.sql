explain global select
  c_custkey,
  empty_orders.o_orderkey,
  empty_orders.o_orderstatus,
  empty_orders.o_orderdate
from
  customer left outer join empty_orders on c_custkey = o_orderkey
order by
  c_custkey, o_orderkey;