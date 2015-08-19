select
  upper(c_name) as c_name, count(1)
from
  customer_parts
where
  c_name is not null and
  c_nationkey = 1
group by
  c_name;