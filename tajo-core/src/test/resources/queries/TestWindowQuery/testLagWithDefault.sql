SELECT
  lag(l_shipmode, 1, 'default') over (PARTITION BY L_ORDERKEY order by l_shipmode ) as shipmode_lag,
  lag(l_linenumber, 1, 100) over (PARTITION BY L_ORDERKEY order by l_shipmode ) as linenumber_lag,
  lag(l_suppkey_t, 1, 1000::int8) over (PARTITION BY L_ORDERKEY order by l_shipmode ) as suppkey_lag,
  lag(l_shipdate_t, 1, '15-01-01'::date) over (PARTITION BY L_ORDERKEY order by l_shipmode ) as shipdate_lag,
  lag(l_commitdate_t, 1, '15-01-01 12:00:00'::timestamp) over (PARTITION BY L_ORDERKEY order by l_shipmode ) as commitdate_lag,
  lag(l_extendedprice, 1, 1.234::float8) over (PARTITION BY L_ORDERKEY order by l_shipmode ) as extendedprice_lag,
  lag(l_discount_t, 1, 0.11::float4) over (PARTITION BY L_ORDERKEY order by l_shipmode ) as discount_lag,
  l_orderkey
FROM
(
  SELECT
    l_orderkey,l_partkey,l_suppkey::INT8 as l_suppkey_t,l_linenumber,l_quantity,
    l_extendedprice,l_discount::FLOAT4 as l_discount_t,l_tax,l_returnflag,l_linestatus,
    l_shipdate::DATE as l_shipdate_t,l_commitdate::TIMESTAMP as l_commitdate_t,l_receiptdate,l_shipinstruct,l_shipmode,l_comment
  FROM
    LINEITEM
) xx