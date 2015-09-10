SELECT unioninput.*
FROM (
  select
    table1.key,
    table1.value
  FROM
    table1
  WHERE
    table1.key < 100

  UNION ALL

  SELECT
    table2.*
  FROM
    table2
  WHERE
    table2.key > 100
) unioninput