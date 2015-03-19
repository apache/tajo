SELECT
  t1.user.id,
  t1.user.name,
  t2.user.id,
  t2.user.name
FROM
  tweets t1 join tweets t2 ON t1.user.id = t2.user.id