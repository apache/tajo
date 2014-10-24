create table hbase_mapped_table (rk int8, col1 int8)
using hbase with ('table'='hbase_table', 'columns'=':key,col1:a#b') as
select l_orderkey, l_partkey
from default.lineitem
