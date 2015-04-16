SELECT
  l_returnflag,
  l_linestatus

FROM (
  SELECT
    *
  FROM
    lineitem
  WHERE
    l_returnflag = 'R'

  UNION

  SELECT
    *
  FROM
    lineitem
  WHERE
    l_returnflag = 'R'
) T
GROUP BY
  l_returnflag,
  l_linestatus;