select
  count(distinct l_orderkey) as v0,
  sum(l_orderkey) as v1,
  sum(l_linenumber) as v2,
  count(*) as v4
from
  lineitem
group by
  l_orderkey;