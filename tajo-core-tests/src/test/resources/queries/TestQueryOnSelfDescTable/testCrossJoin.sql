select
  user.favourites_count::int8,
  l_linenumber,
  l_comment
from
  default.lineitem, self_desc_table3