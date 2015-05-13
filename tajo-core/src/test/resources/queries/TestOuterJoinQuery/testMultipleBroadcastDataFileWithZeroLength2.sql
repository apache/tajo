select b.o_orderkey, b.o_orderdate, b.o_custkey, a.c_custkey, a.c_name, c.n_nationkey, c.n_name
from customer_large a
 left outer join orders_large b on a.c_custkey = b.o_custkey
 left outer join nation_multifile c on a.c_nationkey = c.n_nationkey
 where c.n_nationkey is not null