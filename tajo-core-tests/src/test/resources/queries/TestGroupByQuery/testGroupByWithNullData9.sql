select l_orderkey, count(distinct l_linenumber) as unique_key
from lineitem
where l_orderkey = 1000
group by l_orderkey