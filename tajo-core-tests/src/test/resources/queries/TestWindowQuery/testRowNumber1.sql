SELECT
  l_orderkey,
  row_number() OVER () as row_num
FROM
  LINEITEM;