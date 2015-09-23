select
  self_desc_table3.user.favourites_count::int8,
  l_linenumber,
  l_comment
from
  default.lineitem, self_desc_table1, self_desc_table3, default.orders, default.supplier
where
  self_desc_table3.user.favourites_count::int8 = (l_orderkey - 1) and l_orderkey = o_orderkey and l_linenumber = s_suppkey and self_desc_table3.user.favourites_count <> self_desc_table1.name.first_name