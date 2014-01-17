SELECT
  count(*)
FROM (
  SELECT
    *
  FROM
    lineitem

  UNION ALL

  SELECT
    *
  FROM
    lineitem
) T