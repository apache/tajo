select
  name, addr, count(1)
from
  table1
group by
  name, addr
order by
  name, addr;