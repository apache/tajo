insert overwrite into table1 select col1, col2, sum(col3) from table2 group by col1, col2
