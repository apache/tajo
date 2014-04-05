select
    l_orderkey,
    p_name,
    n_name
from
    lineitem_large,
    orders,
    part,
    customer_large,
    nation
where
    l_orderkey = o_orderkey
    and l_partkey = p_partkey
    and o_custkey = c_custkey
    and c_nationkey = n_nationkey