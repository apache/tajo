create table testCtasWithColumnedPartition (col1 float, col2 float) partition by column(key float8) as
select l_orderkey as col1, l_partkey as col2, l_quantity as key from lineitem;