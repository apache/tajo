select a.l_orderkey, b.c_custkey, b.c_name, b.c_nationkey, d.n_name
from lineitem a
left outer join customer b on a.l_orderkey = b.c_custkey
left outer join orders c on b.c_custkey = c.o_custkey
left outer join nation d on a.l_orderkey = d.n_nationkey