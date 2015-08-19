select
  r_regionkey,
  case
    when r_regionkey = 1 then 10 + r_regionkey
    when r_regionkey = 2 then 10 + r_regionkey
    when r_regionkey = 3 then 10 + r_regionkey
    when r_regionkey = 4 then 10 + r_regionkey
  end as cond
from
  region;