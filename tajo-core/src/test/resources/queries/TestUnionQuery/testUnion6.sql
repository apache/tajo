SELECT
  count(*)
FROM (
  SELECT
    *
  FROM
    lineitem

  UNION

  SELECT
    *
  FROM
    lineitem
) T