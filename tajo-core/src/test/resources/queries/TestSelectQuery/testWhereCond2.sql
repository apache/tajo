select *
from (
 select a.l_orderkey, count(*) as cnt
  from lineitem a
  group by a.l_orderkey
) t
where t.cnt > 0
order by t.l_orderkey