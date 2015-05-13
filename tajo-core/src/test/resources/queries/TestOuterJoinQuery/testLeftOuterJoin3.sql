select a.c_custkey, a.c_name, a.c_nationkey, b.l_orderkey, c.o_orderdate, d.o_orderdate, e.n_name, f.p_name
from customer_large a
left outer join lineitem_large b on a.c_custkey = b.l_orderkey
left outer join orders c on b.l_orderkey = c.o_orderkey
left outer join orders_large d on a.c_custkey = d.o_orderkey
left outer join nation e on d.o_orderkey = e.n_nationkey
left outer join part f on f.p_partkey = d.o_orderkey
