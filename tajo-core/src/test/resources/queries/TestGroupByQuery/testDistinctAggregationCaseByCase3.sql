select
  col1,
  count(distinct col2) as cnt1,
  count(distinct case when col3 is not null then col2 else null end) as cnt2
from
  table10
group by
  col1;