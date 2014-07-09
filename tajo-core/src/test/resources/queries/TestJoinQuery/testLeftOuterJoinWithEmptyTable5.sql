select
  l_linenumber,
  sum(empty_orders.o_orderkey),
  max(empty_orders.o_orderstatus),
  max(empty_orders.o_orderdate),
  avg(l_quantity),
  sum(l_quantity)
from
  lineitem left outer join empty_orders on l_orderkey = o_orderkey
  group by l_linenumber
order by l_linenumber ;