 SELECT
  dt,
  sum(xy) over(partition by dt)
FROM
  sum_example;