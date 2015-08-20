select
  a.c_custkey,
  123::INT8 as const_val,
  b.min_name
from
  customer a
left outer join (
  select
    c_custkey,
    min(c_name) as min_name
    from customer
  group by
    c_custkey)
  b
on a.c_custkey = b.c_custkey
order by
  a.c_custkey;