select
  num
from (

select
  o_custkey as num
from
  orders

union

select
  c_custkey as num
from
  customer
) table1

order by
  num;