select
  l_orderkey,
  count(*) as cnt,
  row_number() over (order by count(*) desc) row_num
from
  lineitem
where
  l_orderkey != 1
group by
  l_orderkey