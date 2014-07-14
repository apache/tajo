 SELECT
  dt,
  sum(xy) over(order by dt)
FROM
  sum_example;