insert overwrite into table1 (col1, col2, col3) select col1, col2, sum(col3) as total from table2 group by col1, col2
