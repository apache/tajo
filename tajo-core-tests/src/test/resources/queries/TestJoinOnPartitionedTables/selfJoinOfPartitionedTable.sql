select
  t1.c_nationkey,
  t2.c_nationkey
from
  customer_parts t1, customer_parts t2
where
  t1.c_nationkey = t2.c_nationkey
order by
  t1.c_nationkey desc;