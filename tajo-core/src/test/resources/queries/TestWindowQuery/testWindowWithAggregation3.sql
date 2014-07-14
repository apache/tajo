select
  count(*) as cnt,
  row_number() over (order by count(*) desc) row_num
from
  lineitem