select
  coordinates,
  avg(retweet_count::int4)
from
  self_desc_table3
group by
  coordinates