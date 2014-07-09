select
  r_name,
  r_regionkey,
  n_name,
  n_regionkey
from
  region left outer join nation on n_regionkey = r_regionkey and r_name in ('AMERICA', 'ASIA')
order by r_name;