select count(distinct l_linenumber) as unique_key, count(distinct l_returnflag || l_linestatus) flag
from lineitem
where l_orderkey = 1000