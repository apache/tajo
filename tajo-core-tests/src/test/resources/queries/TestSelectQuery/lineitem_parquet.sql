create table lineitem_parquet (
	l_orderkey INT8,
	l_partkey INT8,
	l_suppkey INT8,
	l_linenumber INT4,
	l_quantity FLOAT8,
	l_extendedprice FLOAT8,
	l_discount FLOAT8,
	l_tax FLOAT8,
	l_returnflag TEXT,
	l_linestatus TEXT,
	l_shipdate TEXT,
	l_commitdate TEXT,
	l_receiptdate TEXT,
	l_shipinstruct TEXT,
	l_shipmode TEXT,
	l_comment TEXT
) using parquet as
select * from lineitem