 SELECT
  l_orderkey,
  sum(l_partkey) over()
FROM
  lineitem;