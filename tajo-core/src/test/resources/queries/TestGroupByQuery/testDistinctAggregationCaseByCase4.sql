select
  col1,
  count(distinct col2) as cnt
from
  table11
group by
  col1;