-- Large customer Table
-- It is used for broadcast join

create external table if not exists customer_large (
    c_custkey INT4, c_name TEXT, c_address TEXT, c_nationkey INT4,
    c_phone TEXT, c_acctbal FLOAT8, c_mktsegment TEXT, c_comment TEXT)
using csv with ('csvfile.delimiter'='|', 'csvfile.null'='NULL') location ${table.path};