select
  c_custkey,
  empty_orders.o_orderkey
from
  empty_orders full outer join customer on c_custkey = o_orderkey
order by
  c_custkey,
  empty_orders.o_orderkey;
