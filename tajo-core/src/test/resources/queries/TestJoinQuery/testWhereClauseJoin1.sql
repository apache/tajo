select
  n_name,
  r_name,
  n_regionkey,
  r_regionkey
from
  nation,
  region
where
  n_regionkey = r_regionkey
order by n_name