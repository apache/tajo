create external table if not exists customer_parquet (
    c_custkey INT4, c_name TEXT, c_address TEXT, c_nationkey INT4,
    c_phone TEXT, c_acctbal FLOAT8, c_mktsegment TEXT, c_comment TEXT)
using parquet location ${table.path};