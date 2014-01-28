create table testCtasWithColumnedPartition (col1 int4, col2 int4) partition by column(key float8) as
select l_orderkey as col1, l_partkey as col2, l_quantity as key from lineitem;