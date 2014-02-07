select
  c_custkey,
  c_name,
  c_nationkey,
  n_nationkey
from
  customer_parts, nation
where
  c_nationkey = n_nationkey
order by
  c_custkey;