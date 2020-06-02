select
  l_orderkey,
  sum(l_extendedprice*(1-l_discount)) as revenue,
  o_orderdate,
  o_shippriority
from
  customer,
  orders,
  lineitem
where
  c_mktsegment in ('BUILDING', 'AUTOMOBILE', 'MACHINERY', 'HOUSEHOLD') -- modified for selectivity
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1996-12-31' -- modified for selectivity
  and l_shipdate > date '1992-03-15' -- modified for selectivity
group by
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  revenue desc,
  o_orderdate;