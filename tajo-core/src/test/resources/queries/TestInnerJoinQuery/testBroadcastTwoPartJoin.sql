select
    l_orderkey,
    p_name,
    n_name
from
    lineitem,
    orders,
    part,
    customer,
    nation
where
    l_orderkey = o_orderkey
    and l_partkey = p_partkey
    and o_custkey = c_custkey
    and c_nationkey = n_nationkey
order by
    l_orderkey,
    p_name,
    n_name
