SELECT
  l_orderkey,
  l_discount,
  rank() OVER (ORDER BY l_discount) r1
FROM
  LINEITEM;