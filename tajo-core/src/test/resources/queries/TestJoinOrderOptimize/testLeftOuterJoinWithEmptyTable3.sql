explain global select count(*)
from (
  select
    c_custkey,
    sum(empty_orders.o_orderkey) as total1,
    max(empty_orders.o_orderstatus) as total2,
    max(empty_orders.o_orderdate) as total3
  from
    customer left outer join empty_orders on c_custkey = o_orderkey
    group by c_custkey
) t1
group by
  c_custkey
order by c_custkey  ;