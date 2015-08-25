 SELECT
  dt,
  dt2,
  row_number() over(window1),
  rank() over(window1)
FROM
  sum_example
WINDOW
  window1 AS (partition by round(dt),dt2 order by ceil(dt) asc null last ROWS BETWEEN 1 PRECEDING AND CURRENT ROW);