select
  n_regionkey, count(*)
from
  customer, lineitem, orders, supplier, nation
where
  l_orderkey = o_orderkey and
  c_custkey = o_custkey and
  l_linenumber = s_suppkey and
  l_partkey in (
    select
      l_partkey
    from
      lineitem
    where
      l_linenumber in (1, 3, 5, 7, 9)
  ) and
  n_nationkey = c_nationkey
group by
  n_regionkey
order by
  n_regionkey
limit 100;