select l_orderkey
, count(*) as cnt1
, count(distinct l_orderkey) as cnt2
, count(distinct l_linenumber) as cnt3
, count(distinct l_returnflag) as cnt4
from lineitem
group by l_orderkey;