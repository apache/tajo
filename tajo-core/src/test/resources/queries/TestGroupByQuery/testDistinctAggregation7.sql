select
    count(*),
    count(distinct c_nationkey),
    count(distinct c_mktsegment)
from
    customer