select
    l_orderkey,
    a.o_custkey,
    p_name
from
    lineitem_large,
    part,
    (select o_orderkey, o_custkey from orders) a
where
    l_partkey = p_partkey
    and l_orderkey = a.o_orderkey
order by l_orderkey, o_custkey, p_name