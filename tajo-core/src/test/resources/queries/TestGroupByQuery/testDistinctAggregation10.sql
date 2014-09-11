select l_orderkey, count(distinct l_orderkey) as cnt2
, count(distinct l_linenumber) as cnt3
, count(*) as cnt1
, count(distinct l_returnflag) as cnt4
, sum(l_discount) as sum1
from lineitem
group by l_orderkey;