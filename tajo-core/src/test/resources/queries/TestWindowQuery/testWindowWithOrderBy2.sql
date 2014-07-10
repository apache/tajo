SELECT
  l_orderkey,
  l_partkey,
  rank() OVER (PARTITION BY L_ORDERKEY ORDER BY l_partkey) r1
FROM
  LINEITEM;