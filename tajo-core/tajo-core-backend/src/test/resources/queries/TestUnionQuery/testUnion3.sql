select
  total
from (

select
  count(*) as total
from
  orders

union

select
  count(*) as total
from
  customer
) table1

order by
  total;