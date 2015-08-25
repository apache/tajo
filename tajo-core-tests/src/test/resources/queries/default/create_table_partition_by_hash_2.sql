CREATE TABLE sales ( col1 int, col2 int)
PARTITION BY HASH (col1)
(
  PARTITION part1,
  PARTITION part2,
  PARTITION part3
);