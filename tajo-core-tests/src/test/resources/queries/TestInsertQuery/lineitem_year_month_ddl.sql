create table lineitem_year_month (
 l_orderkey bigint,
 l_partkey bigint,
 l_suppkey bigint,
 l_linenumber int,
 l_quantity float8,
 l_extendedprice float8,
 l_discount float8,
 l_tax float8,
 l_returnflag text,
 l_linestatus text,
 l_shipdate text,
 l_commitdate text,
 l_receiptdate text,
 l_shipinstruct text,
 l_shipmode text,
 l_comment text
) PARTITION BY COLUMN (year TEXT, MONTH TEXT);