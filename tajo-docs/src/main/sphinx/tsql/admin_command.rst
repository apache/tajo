*********************************
Administration Commands
*********************************


==========
Synopsis
==========

Tsql provides commands for administrator as follows:

.. code-block:: sql

  default> \admin;
  usage: admin [options]
   -cluster          Show Cluster Info
   -desc             Show Query Description
   -h,--host <arg>   Tajo server host
   -kill <arg>       Kill a running query
   -list             Show Tajo query list
   -p,--port <arg>   Tajo server port
   -showmasters      gets list of tajomasters in the cluster


-----------------------------------------------
Basic usages
-----------------------------------------------

``-list`` option shows a list of all running queries as follows: ::

  default> \admin -list
  QueryId              State               StartTime           Query
  -------------------- ------------------- ------------------- -----------------------------
  q_1411357607375_0006 QUERY_RUNNING       2014-09-23 07:19:40 select count(*) from lineitem


``-desc`` option shows a detailed description of a specified running query as follows: ::

  default> \admin -desc q_1411357607375_0006
  Id: 1
  Query Id: q_1411357607375_0006
  Started Time: 2014-09-23 07:19:40
  Query State: QUERY_RUNNING
  Execution Time: 20.0 sec
  Query Progress: 0.249
  Query Statement:
  select count(*) from lineitem


``-kill`` option kills a specified running query as follows: ::

  default> \admin -kill q_1411357607375_0007
  q_1411357607375_0007 is killed successfully.



``-showmasters`` option shows a list of all tajo masters as follows: ::

  default> \admin -showmasters
  grtajo01