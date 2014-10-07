*********************************
Executing as background process
*********************************


If you execute tsql as a background process, tsql will exit before executing a query because of some limitation of Jline2.

Example: 

 .. code-block:: sql

  $ bin/tsql  -f aggregation.sql &
  [1] 19303
  $
  [1]+  Stopped                 ./bin/tsql -f aggregation.sql


To avoid above problem, Tajo provides the -B command as follows:

.. code-block:: sql

  $ bin/tsql  -B -f aggregation.sql &
    [2] 19419
    Progress: 0%, response time: 0.218 sec
    Progress: 0%, response time: 0.22 sec
    Progress: 0%, response time: 0.421 sec
    Progress: 0%, response time: 0.823 sec
    Progress: 0%, response time: 1.425 sec
    Progress: 1%, response time: 2.227 sec
