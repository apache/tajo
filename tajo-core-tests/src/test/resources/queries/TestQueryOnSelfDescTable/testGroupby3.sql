select
  user.time_zone,
  sum(user.favourites_count::int8)
from
  self_desc_table3
group by
  user.time_zone