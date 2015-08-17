select
  n_regionkey + n_nationkey as v1,
  n_regionkey,
  r_regionkey,
  (r_regionkey + n_regionkey) as plus1,
  (r_regionkey + n_regionkey) as plus2,
  ((r_regionkey + n_regionkey) / 2) as result,
  sum(r_regionkey + n_regionkey) as total
from
  region,
  nation
where
  r_regionkey = n_regionkey and r_regionkey > 0
group by
  n_regionkey + n_nationkey,
  n_regionkey,
  r_regionkey,
  (r_regionkey + n_regionkey),
  ((r_regionkey + n_regionkey) / 2)

order by
  n_regionkey + n_nationkey, n_regionkey;