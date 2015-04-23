select l_orderkey, avgpy(l_partkey) total, sum(l_linenumber) as num from lineitem
group by l_orderkey
having avgpy(l_partkey) = 2.5 or num = 1;