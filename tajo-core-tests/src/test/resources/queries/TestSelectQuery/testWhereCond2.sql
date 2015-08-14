select *
from (
 select a.l_orderkey, count(*) as cnt, sum(l_extendedprice) as sum1
  from lineitem a
  group by a.l_orderkey
  having sum1 > 70000
) t
where t.cnt > 1
order by t.l_orderkey