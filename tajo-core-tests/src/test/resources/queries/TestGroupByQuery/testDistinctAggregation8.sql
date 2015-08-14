select
  sum(distinct l_orderkey),
  l_linenumber, l_returnflag, l_linestatus, l_shipdate,
  count(distinct l_partkey),
  sum(l_orderkey)
from
  lineitem
group by
  l_linenumber, l_returnflag, l_linestatus, l_shipdate;