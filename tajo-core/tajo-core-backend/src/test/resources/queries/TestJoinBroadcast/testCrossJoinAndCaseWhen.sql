select
  r_regionkey,
  n_regionkey,
  case
    when r_regionkey = 1 then 'one'
    when r_regionkey = 2 then 'two'
    when r_regionkey = 3 then 'three'
    when r_regionkey = 4 then 'four'
    else 'zero'
  end as cond
from
  region,
  nation
where
  r_regionkey = n_regionkey
order by
  r_regionkey,
  n_regionkey