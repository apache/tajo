SELECT
  lead(l_shipmode) over (PARTITION BY L_ORDERKEY order by l_shipmode ) as shipmode_lead,
  lead(l_linenumber) over (PARTITION BY L_ORDERKEY order by l_shipmode ) as linenumber_lead,
  lead(l_suppkey_t) over (PARTITION BY L_ORDERKEY order by l_shipmode ) as suppkey_lead,
  lead(l_shipdate_t) over (PARTITION BY L_ORDERKEY order by l_shipmode ) as shipdate_lead,
  lead(l_commitdate_t) over (PARTITION BY L_ORDERKEY order by l_shipmode ) as commitdate_lead,
  lead(l_extendedprice) over (PARTITION BY L_ORDERKEY order by l_shipmode ) as extendedprice_lead,
  lead(l_discount_t) over (PARTITION BY L_ORDERKEY order by l_shipmode ) as discount_lead,
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