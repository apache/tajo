select
  l_orderkey,
  count(*) as cnt,
  row_number() over (order by count(*) desc, l_orderkey) row_num
from
  lineitem
group by
  l_orderkey
order by
  l_orderkey;