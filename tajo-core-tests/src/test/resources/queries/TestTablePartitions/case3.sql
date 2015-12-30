select
  l.l_orderkey,
  p.col1,
  key
  from lineitem as l, testQueryCasesOnColumnPartitionedTable as p
where
  (key = 45.0 or key = 38.0) and l.l_orderkey = p.col1
order by
   l.l_orderkey,
   p.col1,
   key


