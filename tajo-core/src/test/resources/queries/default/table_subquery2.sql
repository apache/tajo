SELECT
  t1.fk,
  t2.name
FROM
 (
    SELECT
      table1.fk
    FROM
      table1
 ) t1

 inner join

 (
    SELECT
      table2.name
    FROM
      table2
 ) t2

 ON t1.fk = t2.fk;