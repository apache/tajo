select * from (
select
  r_regionkey,
  n_regionkey,
  (r_regionkey + n_regionkey) as plus
from
  region,
  nation
where
  r_regionkey = n_regionkey
order by
  r_regionkey, n_regionkey
) where plus > 10