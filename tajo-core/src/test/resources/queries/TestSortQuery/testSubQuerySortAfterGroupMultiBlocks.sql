select l_orderkey, revenue from (
  select l_orderkey, revenue from (
    select l_orderkey, sum(l_extendedprice*l_discount) as revenue from lineitem group by l_orderkey
  ) l1
) l2 order by l_orderkey