select
  l_orderkey, l_linenumber
from (
  select distinct l_orderkey, l_linenumber from lineitem
) table1
order by
  l_orderkey, l_linenumber;