*********************************
Executing Queries from Files
*********************************


-----------------------------------------------
Basic usages
-----------------------------------------------


Tajo can execute more queries that were saved to a file using the -f file argument as follows:

.. code-block:: sql

  $ cat aggregation.sql
  select count(*) from table1;
  select sum(score) from table1;

  $ bin/tsql -f aggregation.sql
  Progress: 0%, response time: 0.216 sec
  Progress: 0%, response time: 0.217 sec
  Progress: 100%, response time: 0.331 sec
  ?count
  -------------------------------
  5
  (1 rows, 0.331 sec, 2 B selected)
  Progress: 0%, response time: 0.203 sec
  Progress: 0%, response time: 0.204 sec
  Progress: 50%, response time: 0.406 sec
  Progress: 100%, response time: 0.769 sec
  ?sum
  -------------------------------
  15.0
  (1 rows, 0.769 sec, 5 B selected)



-----------------------------------------------
Setting parameter value in SQL file
-----------------------------------------------

If you wish to set a parameter value in the SQL file, you can set with the -param key=value option. When you use this feature, you have to use the parameter in the file as follows:

.. code-block:: sql

  ${paramter name}


You have to put the parameter name in braces and you must use the $ symbol for the prefix as follows:

.. code-block:: sql

  $ cat aggregation.sql
  select count(*) from table1 where id = ${p_id};

  $ bin/tsql -param p_id=1 -f aggregation.sql
  Progress: 0%, response time: 0.216 sec
  Progress: 0%, response time: 0.217 sec
  Progress: 100%, response time: 0.331 sec
  ?count
  -------------------------------
  1
  (1 rows, 0.331 sec, 2 B selected)
