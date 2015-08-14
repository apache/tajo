select sum(cnt1), sum(sum2)
from (
  select o_orderdate, count(distinct o_orderpriority), count(distinct o_orderkey) cnt1, sum(o_totalprice) sum2
  from orders group by o_orderdate
) a