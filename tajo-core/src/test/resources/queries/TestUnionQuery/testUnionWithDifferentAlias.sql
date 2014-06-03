SELECT
  col1,
  col2

FROM (
  SELECT
    l_returnflag col1, l_linestatus col2, l_orderkey col3
  FROM
    lineitem
  WHERE
    l_returnflag = 'N'

  UNION ALL

  SELECT
    l_returnflag col2, l_linestatus col5, l_orderkey col6
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