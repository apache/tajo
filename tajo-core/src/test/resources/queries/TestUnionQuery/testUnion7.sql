SELECT
  orderkey
FROM (
  SELECT
    l_orderkey as orderkey
  FROM
    lineitem

  UNION

  SELECT
    l_orderkey as orderkey
  FROM
    lineitem
) T

order by
  orderkey;