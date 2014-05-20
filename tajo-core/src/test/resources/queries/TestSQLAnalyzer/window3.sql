 SELECT
  dt,
  sum(xy) over(partition by round(dt))
FROM
  sum_example;