select
  l_orderkey,
  count(*) as cnt,
  row_number() over (partition by l_orderkey order by count(*) desc)
  row_num
from
  lineitem
group by
  l_orderkey