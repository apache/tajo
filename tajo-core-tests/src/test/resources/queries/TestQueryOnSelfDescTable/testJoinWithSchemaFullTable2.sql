select
  user.favourites_count::int8,
  l_linenumber,
  l_comment
from
  default.lineitem, self_desc_table3, default.orders, default.supplier
where
  user.favourites_count::int8 = (l_orderkey - 1) and l_orderkey = o_orderkey and l_linenumber = s_suppkey