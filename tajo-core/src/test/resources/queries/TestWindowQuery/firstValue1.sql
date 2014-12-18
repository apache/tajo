SELECT
  l_orderkey,
  first_value(l_shipmode) over (PARTITION BY L_ORDERKEY order by l_shipmode ) as shipmode_first
FROM
  LINEITEM