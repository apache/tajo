select max(l_orderkey) as maximum, count(l_linenumber) as unique_key
from lineitem
where l_orderkey = 1000