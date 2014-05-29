select col1, cnt
from (
    select L_RETURNFLAG as col1, count(*) as cnt from lineitem group by col1
    union all
    select cast(n_regionkey as TEXT) as col1, count(*) as cnt from nation group by col1
) a
where a.cnt > 1
order by a.col1