 SELECT
  dt,
  sum(xy) over()
FROM
  sum_example;