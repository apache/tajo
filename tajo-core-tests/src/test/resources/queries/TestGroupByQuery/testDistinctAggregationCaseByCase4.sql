select
  col1,
  count(distinct col2) as cnt
from
  testDistinctAggregationCaseByCase4
group by
  col1;