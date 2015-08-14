select
  *
from (
  select
    case when id > 2 then null else id end as col1,
    name as col2
  from
    nullsort
) a

order by
  col1,
  col2;