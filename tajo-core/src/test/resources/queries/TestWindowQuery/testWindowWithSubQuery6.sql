select
  a.r_name, a.row_num
from (
  select
    r_name,
    r_regionkey,
    row_number() over (order by r_name) as row_num
  from
    region a
) a join region b on a.r_regionkey = b.r_regionkey
where
  a.row_num >= 3;