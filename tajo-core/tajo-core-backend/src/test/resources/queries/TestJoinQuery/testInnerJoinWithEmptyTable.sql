select
  c_custkey,
  orders.o_orderkey
from
  customer, empty_orders
where c_custkey = o_orderkey
order by
  c_custkey, o_orderkey;