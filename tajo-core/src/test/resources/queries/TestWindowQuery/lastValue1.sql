SELECT
  l_orderkey,
  last_value(l_shipmode) over (PARTITION BY L_ORDERKEY order by l_shipmode ) as shipmode_last,
  last_value(l_linenumber) over (PARTITION BY L_ORDERKEY order by l_linenumber ) as linenumber_last,
  last_value(l_suppkey_t) over (PARTITION BY L_ORDERKEY order by l_suppkey_t ) as suppkey_last,
  last_value(l_shipdate_t) over (PARTITION BY L_ORDERKEY order by l_shipdate_t ) as shipdate_last,
  last_value(l_commitdate_t) over (PARTITION BY L_ORDERKEY order by l_commitdate_t ) as commitdate_last,
  last_value(l_extendedprice) over (PARTITION BY L_ORDERKEY order by l_extendedprice ) as extendedprice_last,
  last_value(l_discount_t) over (PARTITION BY L_ORDERKEY order by l_discount_t ) as discount_last
FROM
(
  SELECT
    l_orderkey,l_partkey,l_suppkey::INT8 as l_suppkey_t,l_linenumber,l_quantity,
    l_extendedprice,l_discount::FLOAT4 as l_discount_t,l_tax,l_returnflag,l_linestatus,
    l_shipdate::DATE as l_shipdate_t,l_commitdate::TIMESTAMP as l_commitdate_t,l_receiptdate,l_shipinstruct,l_shipmode,l_comment
  FROM
    LINEITEM
) xx