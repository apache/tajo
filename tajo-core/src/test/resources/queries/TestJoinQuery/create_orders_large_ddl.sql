-- Large Orders Table
-- It is used for broadcast join

create external table if not exists orders_large ( o_orderkey INT4, o_custkey INT4,
   o_orderstatus TEXT, o_totalprice FLOAT8, o_orderdate TEXT,
   o_orderpriority TEXT, o_clerk TEXT, o_shippriority INT4, o_comment TEXT)
using csv with ('csvfile.delimiter'='|', 'csvfile.null'='NULL') location ${table.path};