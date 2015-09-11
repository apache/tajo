SELECT DISTINCT
  c_custkey,
  orders.o_orderkey,
  orders.o_orderstatus,
  orders.o_orderdate
from
  customer left outer join orders on c_custkey = o_orderkey
order by
  c_custkey,
  o_orderkey;