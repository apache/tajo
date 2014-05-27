select
  c_custkey,
  orders.o_orderkey,
  coalesce(orders.o_orderstatus, 'N/A'),
  orders.o_orderdate
from
  customer left outer join orders on c_custkey = o_orderkey
where orders.o_orderkey = 100
order by
  c_custkey, o_orderkey;