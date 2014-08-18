select
  'day',
  l_orderkey,
  count(*) as sum
from
  lineitem
group by
  'day',
  l_orderkey
order by
  'day',
  l_orderkey;