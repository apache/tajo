select
  *
from
  lineitem, orders
where
  l_orderkey = o_orderkey and
  l_partkey in (select l_partkey from lineitem where l_linenumber in (1, 3, 5, 7, 9))