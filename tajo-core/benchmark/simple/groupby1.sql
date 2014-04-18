select
	l_orderkey, sum(l_quantity) as sum_qty, max(l_quantity) as max_qty, min(l_quantity) as min_qty
from
	lineitem
group by
    l_orderkey