select
    sum(l_quantity) as quantity,
    count(distinct l_suppkey) suppkey,
    l_returnflag
from
    lineitem
group by l_returnflag