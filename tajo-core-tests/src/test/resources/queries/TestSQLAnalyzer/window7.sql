 SELECT
  dt,
  dt2,
  row_number() over(partition by round(dt),dt2 order by ceil(dt) asc nulls last ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM
  sum_example;