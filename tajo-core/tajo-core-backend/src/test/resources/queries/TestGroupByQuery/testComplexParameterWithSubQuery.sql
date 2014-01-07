select
  count(*) as total
from (
  select * from lineitem
  union all
  select * from lineitem ) l;