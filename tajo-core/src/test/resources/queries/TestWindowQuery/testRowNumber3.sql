SELECT
  l_orderkey,
  row_number() OVER (PARTITION BY L_ORDERKEY) as row_num,
  l_discount,
  avg(l_discount) OVER (PARTITION BY L_ORDERKEY) as average
FROM
  LINEITEM