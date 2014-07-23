  select
    l_orderkey,
    '##' as col1,
    sum(l_orderkey) as s1
  from
    lineitem join orders o1 on l_orderkey = o1.o_orderkey join orders o2 on l_orderkey = o2.o_orderkey
  group by
    l_orderkey, col1
  order by
    l_orderkey;