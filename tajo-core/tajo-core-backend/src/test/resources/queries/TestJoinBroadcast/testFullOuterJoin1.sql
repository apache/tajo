select
  c_custkey,
  orders.o_orderkey
from
  orders full outer join customer on c_custkey = o_orderkey
order by
  c_custkey,
  orders.o_orderkey;
