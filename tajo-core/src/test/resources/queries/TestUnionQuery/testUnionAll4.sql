select
  count(*)
from (

  select
    count(*) as total
  from
    orders

  union all

  select
    count(*) as total
  from
    customer
) table1;