select
  timestamp '2014-07-07 04:28:31.561' as b,
  '##' as c,
  count(*) d
from
  lineitem
group by
  b, c;