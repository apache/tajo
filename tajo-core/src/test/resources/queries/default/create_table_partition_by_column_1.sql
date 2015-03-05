CREATE TABLE sales ( col1 int, col2 int) USING RCFILE
PARTITION BY COLUMN (col3 int, col4 float, col5 text);


