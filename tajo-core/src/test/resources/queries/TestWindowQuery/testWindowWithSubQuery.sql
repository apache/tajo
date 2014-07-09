select
  r_name,
  c,
  rank() over (order by r_regionkey) as ran
from (
  select
    r_name,
    r_regionkey,
    count(*) as c
  from
    region
  group by
    r_name, r_regionkey
) a;