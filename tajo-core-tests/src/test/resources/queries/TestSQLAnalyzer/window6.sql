 SELECT
  dt,
  dt2,
  sum(xy) over(partition by round(dt),dt2 order by ceil(dt) asc nulls last ROWS UNBOUNDED PRECEDING)
FROM
  sum_example;