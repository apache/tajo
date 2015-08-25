select
  l_orderkey,
  l_quantity,
  rank() over (partition by l_orderkey) as r,
  5 as const_val
from
  lineitem;