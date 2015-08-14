SELECT
  A.n_regionkey, B.r_regionkey, A.n_name, B.r_name
FROM
  (SELECT * FROM nation WHERE n_name LIKE 'A%') A
  JOIN region B ON A.n_regionkey=B.r_regionkey;