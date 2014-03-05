************************
First query execution
************************

First of all, we need to prepare some data for query execution. For example, you can make a simple text-based table as follows: ::

  $ mkdir /home/x/table1
  $ cd /home/x/table1
  $ cat > data.csv
  1|abc|1.1|a
  2|def|2.3|b
  3|ghi|3.4|c
  4|jkl|4.5|d
  5|mno|5.6|e
  <CTRL + D>

This schema of this table is (int, text, float, text). ::

  $ $TAJO_HOME/bin/tsql
  tajo> create external table table1 (
        id int,
        name text, 
        score float, 
        type text) 
        using csv with ('csvfile.delimiter'='|') location 'file:/home/x/table1';

In order to load an external table, you need to use ‘create external table’ statement. 
In the location clause, you should use the absolute directory path with an appropriate scheme. 
If the table resides in HDFS, you should use ‘hdfs’ instead of ‘file’.

If you want to know DDL statements in more detail, please see Query Language. ::

  tajo> \d
  table1

‘d’ command shows the list of tables. ::

  tajo> \d table1

  table name: table1
  table path: file:/home/x/table1
  store type: CSV
  number of rows: 0
  volume (bytes): 78 B
  schema:
  id      INT
  name    TEXT
  score   FLOAT
  type    TEXT

‘d [table name]’ command shows the description of a given table.

Also, you can execute SQL queries as follows: ::

  tajo> select * from table1 where id > 2;
  final state: QUERY_SUCCEEDED, init time: 0.069 sec, response time: 0.397 sec
  result: file:/tmp/tajo-hadoop/staging/q_1363768615503_0001_000001/RESULT, 3 rows ( 35B)

  id,  name,  score,  type
  - - - - - - - - - -  - - -
  3,  ghi,  3.4,  c
  4,  jkl,  4.5,  d
  5,  mno,  5.6,  e

  tajo>



