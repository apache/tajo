select col1, sum(cnt)
from (
    select col1, cnt
    from (  select l_returnflag col1, count(distinct l_orderkey) cnt from lineitem
            join orders on l_orderkey = o_orderkey and o_custkey > 0
            group by l_returnflag) b
    where col1 = 'N'
    union all
    select cast(n_regionkey as TEXT) as col1, count(*) as cnt from nation
    where n_regionkey > 2
    group by col1
) a
where round(cast(a.cnt as FLOAT4)) > 1.0
group by a.col1
order by a.col1
