SELECT
  n_nationkey,
  n_name

FROM (
  SELECT
    *
  FROM
    nation
  WHERE
    n_regionkey = 0


  UNION

  SELECT
    *
  FROM
    nation
  WHERE
    n_regionkey = 0

) T
GROUP BY
  n_nationkey,
	n_name
ORDER BY
  n_nationkey desc,
  n_name desc;