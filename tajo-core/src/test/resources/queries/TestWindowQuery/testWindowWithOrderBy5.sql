SELECT
  l_orderkey,
  l_partkey,
  rank() OVER (ORDER BY l_orderkey) r1,
  rank() OVER(ORDER BY l_partkey desc) r2
FROM
  LINEITEM
where
  l_partkey > 0 and l_partkey < 100;