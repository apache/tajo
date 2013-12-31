SELECT
  l1.l_orderkey

FROM
  lineitem AS l1

  INNER JOIN lineitem AS l2

  ON l1.l_orderkey = l2.l_orderkey;