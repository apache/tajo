*********************************
Executing a single command
*********************************


You may want to run more queries without entering tsql prompt. Tsql provides the ``-c`` argument for above requirement. And Tajo assumes that queries are separated by semicolon as follows:

.. code-block:: sql

  $ bin/tsql  -c "select count(*) from table1; select sum(score) from table1;"
  Progress: 0%, response time: 0.217 sec
  Progress: 0%, response time: 0.218 sec
  Progress: 100%, response time: 0.317 sec
  ?count
  -------------------------------
  5
  (1 rows, 0.317 sec, 2 B selected)
  Progress: 0%, response time: 0.202 sec
  Progress: 0%, response time: 0.204 sec
  Progress: 100%, response time: 0.345 sec
  ?sum
  -------------------------------
  15.0
  (1 rows, 0.345 sec, 5 B selected)
