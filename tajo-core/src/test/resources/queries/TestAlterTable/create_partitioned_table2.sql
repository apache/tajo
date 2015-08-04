create table partitioned_table2 (col3 float8, col4 text) USING text  WITH ('text.delimiter'='|')
PARTITION by column(col1 int4, col2 int4)