select
  self_desc_table3.user.favourites_count::int8
from
  github, self_desc_table3
where
  self_desc_table3.user.favourites_count = (github.actor.id::int8 - 206379)::text