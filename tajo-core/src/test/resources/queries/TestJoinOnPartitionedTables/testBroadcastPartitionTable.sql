select
  c_custkey,
  c_name,
  c_nationkey,
  n_nationkey,
  o_orderkey
from
  customer_broad_parts,
  nation_multifile,
  orders_multifile
where
  c_nationkey = n_nationkey
and
  o_custkey = c_custkey
order by
  c_custkey;