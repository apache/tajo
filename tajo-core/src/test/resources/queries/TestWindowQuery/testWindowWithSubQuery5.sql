select
  *
from (
  select
    r_name,
    rank() over (order by r_regionkey) as ran
  from
    region
) a
where
  ran >= 3;