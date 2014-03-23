SELECT
  *
FROM (
  SELECT
    "tb1"."aGe",
    "tb1"."teXt",
    "Number"
  FROM
    "TABLE1" "tb1"

  UNION

  SELECT
    "aGe",
    "teXt",
    "Number"
  FROM
    "tablE1"
) T1
ORDER BY
  "aGe";

