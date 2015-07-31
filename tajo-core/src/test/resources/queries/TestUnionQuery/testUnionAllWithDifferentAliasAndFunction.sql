SELECT
  col1,
  col2

FROM (
  SELECT
    l_returnflag col1, concat(l_linestatus, l_shipdate) col2, l_orderkey col3
  FROM
    lineitem
  WHERE
    l_returnflag = 'N'

  UNION ALL

  SELECT
    concat(l_returnflag, l_shipdate) col3, l_linestatus col4, l_orderkey col5
  FROM
    lineitem
  WHERE
    l_returnflag = 'R'
) T

GROUP BY
  col1,
	col2

ORDER BY
  col1,
	col2