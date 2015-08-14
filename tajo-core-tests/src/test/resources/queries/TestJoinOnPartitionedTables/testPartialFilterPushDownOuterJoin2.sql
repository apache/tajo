-- In the case of no partition directory
select a.n_nationkey, a.n_name, b.c_custkey, b.c_nationkey, b.c_name
from nation a
left outer join customer_parts b on a.n_nationkey = b.c_custkey
and b.c_nationkey = 100
