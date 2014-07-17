insert OVERWRITE into T3
  select
    l_orderkey,
    '##' as col1
  from
    lineitem join orders on l_orderkey = o_orderkey
  group by
    l_orderkey, col1
  order by
    l_orderkey;