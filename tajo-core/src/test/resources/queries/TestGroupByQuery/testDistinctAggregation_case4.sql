select
    l_orderkey,
    sum(l_quantity) as quantity,
    count(distinct l_partkey) as partkey,
    count(distinct l_suppkey),
    max(l_quantity),
    l_returnflag
from
    lineitem
group by l_returnflag, l_orderkey