explain global select
  c_custkey,
  o.o_orderkey,
  'val' as val
from
  customer left outer join (select * from orders) o on c_custkey = o.o_orderkey
order by
  c_custkey,
  o_orderkey;