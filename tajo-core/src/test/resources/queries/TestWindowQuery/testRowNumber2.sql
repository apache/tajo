SELECT
  l_orderkey,
  row_number() OVER (PARTITION BY L_ORDERKEY) as row_num
FROM
  LINEITEM