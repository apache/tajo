select
    l_orderkey,
    p_name,
    s_name
from
    lineitem_large,
    part,
    supplier
where
    lineitem_large.l_partkey = part.p_partkey
    and lineitem_large.l_suppkey = supplier.s_suppkey;