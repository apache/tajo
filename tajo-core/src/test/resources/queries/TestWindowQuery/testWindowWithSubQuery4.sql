select
  a.r_name,
  a.r_regionkey,
  row_number() over (partition by a.r_regionkey order by a.cnt desc) mk
from (
  select
    r_name,
    r_regionkey,
    count(*) cnt
  from
    default.region
  group by
    r_name, r_regionkey
) a;