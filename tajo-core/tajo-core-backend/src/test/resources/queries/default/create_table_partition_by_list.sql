CREATE TABLE sales ( col1 int, col2 int)
PARTITION BY LIST (col1)
 (
  PARTITION col1 VALUES ('Seoul', '서울'),
  PARTITION col2 VALUES ('Busan', '부산')
 );


