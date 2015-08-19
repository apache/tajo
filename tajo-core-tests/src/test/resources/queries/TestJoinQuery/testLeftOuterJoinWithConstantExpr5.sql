SELECT
  l.l_orderkey,
  o.o_orderkey,
  '201405' as key1,
  '5-LOW' as key2
from
  lineitem l left outer join orders o on l.l_orderkey = o.o_orderkey
WHERE
  o_orderpriority = '5-LOW'
