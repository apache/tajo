select
  c_custkey,
  empty_orders.o_orderkey
from
  customer, empty_orders
where c_custkey = o_orderkey
order by
  c_custkey, o_orderkey;