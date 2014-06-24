select
  n_name,
  r_name,
  n_nationkey + 1 as p1,
  r_regionkey + 1 as p2
from
  nation, region
where
  n_regionkey = r_regionkey
order by n_name