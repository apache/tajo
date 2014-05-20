select
    lineitem.l_orderkey as l_orderkey,
    lineitem.l_partkey as l_partkey,
    count(distinct lineitem.l_partkey) as cnt1,
    count(distinct lineitem.l_suppkey) as cnt2,
    sum(lineitem.l_quantity) as sum1
from
    lineitem
group by
    lineitem.l_orderkey, lineitem.l_partkey