CREATE TABLE sales (
  col1 int,
  col2 int)
PARTITION BY COLUMN (col3 int, col4 float, col5 text) AS

SELECT
  col1,
  col2,
  col3,
  col4,
  col5
FROM
  sales_src
WHERE
  col1 > 16


