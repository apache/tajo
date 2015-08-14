select
    lineitem.l_orderkey as l_orderkey,
    count(distinct lineitem.l_partkey) as cnt1,
    sum(lineitem.l_quantity + lineitem.l_linenumber)/count(distinct lineitem.l_suppkey) as value2,
    lineitem.l_partkey as l_partkey,
    avg(lineitem.l_quantity) as avg1,
    count(distinct lineitem.l_suppkey) as cnt2
from
    lineitem
group by
    lineitem.l_orderkey, lineitem.l_partkey