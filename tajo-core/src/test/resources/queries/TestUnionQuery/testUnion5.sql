select
  count(*)
from (

  select
    count(*) as total
  from
    orders
  WHERE
    o_orderkey > 0

  union

  select
    count(*) as total
  from
    customer
  WHERE
    c_custkey > 0
) table1;