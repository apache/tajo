select a.l_orderkey, b.c_custkey, b.c_name, b.c_nationkey, d.n_name
from lineitem_large a
left outer join customer_large b on a.l_orderkey = b.c_custkey
left outer join orders c on b.c_custkey = c.o_custkey
left outer join nation d on a.l_orderkey = d.n_nationkey
--inner join customer_large b on a.l_orderkey = b.c_custkey
--inner join orders c on b.c_custkey = c.o_custkey
--inner join nation d on a.l_orderkey = d.n_nationkey
