select
  nation.n_nationkey as n_nationkey,
  customer.c_name as c_name,
  count(nation.n_nationkey) as cnt
from
  nation inner join customer on n_nationkey = c_nationkey
group by
  nation.n_nationkey,
  customer.c_name
order by
  n_nationkey, c_name;