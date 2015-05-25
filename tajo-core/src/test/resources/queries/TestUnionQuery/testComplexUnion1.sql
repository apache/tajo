SELECT
  l_orderkey,
  l_partkey,
  query
FROM
  (
  SELECT
    l_orderkey,
    l_partkey,
    'abc' as query
  FROM
    lineitem
  WHERE
    l_orderkey = 1

  UNION ALL

  SELECT
    l_orderkey,
    l_partkey,
    'bbc' as query
  FROM
    lineitem
  WHERE
    l_orderkey = 1
) result

