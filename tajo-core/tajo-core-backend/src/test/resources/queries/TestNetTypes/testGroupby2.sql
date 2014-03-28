select
  addr,
  count(*)
from
  table1
group by
  addr
order by
  addr;