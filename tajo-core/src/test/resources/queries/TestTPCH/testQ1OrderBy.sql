select
  l_returnflag,
  l_linestatus,
  count(*) as count_order
from
  lineitem
group by
  l_returnflag,
  l_linestatus
order by
  l_returnflag,
  l_linestatus;