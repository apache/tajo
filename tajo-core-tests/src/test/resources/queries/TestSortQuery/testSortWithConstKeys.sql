select
  l_orderkey,
  l_linenumber,
  1 as key1,
  2 as key2
from
  lineitem
order by
  key1,
  key2;