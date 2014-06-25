select c_custkey, c_nationkey, c_name, o_custkey, (case when a.c_nationkey > 3 then 4 else 3 end)
from customer_parts a
inner join orders b
on a.c_custkey = b.o_custkey
where a.c_custkey = (case when a.c_name like 'Customer%' and a.c_nationkey > 3 then 4 else 3 end)