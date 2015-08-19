select b.o_orderkey, b.o_orderdate, b.o_custkey, a.c_custkey, a.c_name, c.n_nationkey, c.n_name
from customer a
 left outer join orders b on a.c_custkey = b.o_custkey
 left outer join nation_partitioned c on a.c_nationkey = c.n_nationkey
 where c.n_nationkey is not null