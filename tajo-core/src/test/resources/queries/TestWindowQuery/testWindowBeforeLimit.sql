select
  r_name,
  rank() over (order by r_regionkey) as ran
from
  region
limit 3;