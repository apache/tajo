*****************************
Tajo Shell (TSQL)
*****************************

==========
Synopsis
==========

.. code-block:: bash

  bin/tsql [options]


Options

* ``-c "quoted sql"`` : Execute quoted sql statements, and then the shell will exist.
* ``-f filename (--file filename)`` : Use the file named filename as the source of commands instead of interactive shell.
* ``-h hostname (--host hostname)`` : Specifies the host name of the machine on which the Tajo master is running.
* ``-p port (--port port)`` : Specifies the TCP port. If it is not set, the port will be 26002 in default. 

===================
Entering tsql shell
===================

If the hostname and the port num are not given, tsql will try to connect the Tajo master specified in ${TAJO_HOME}/conf/tajo-site.xml. ::

  bin/tsql

  tajo>

If you want to connect a specified TajoMaster, you should use '-h' and (or) 'p' options as follows: ::

  bin/tsql -h localhost -p 9004

  tajo> 

===================
 Meta Commands
===================

In tsql, anything command that begins with an unquoted backslash ('\') is a tsql meta-command that is processed by tsql itself.

In the current implementation, there are meta commands as follows: ::

  tajo> \?

  General
    \copyright  show Apache License 2.0
    \version    show Tajo version
    \?          show help
    \q          quit tsql


  Informational
    \d         list tables
    \d  NAME   describe table


  Documentations
    tsql guide        http://wiki.apache.org/tajo/tsql
    Query language    http://wiki.apache.org/tajo/QueryLanguage
    Functions         http://wiki.apache.org/tajo/Functions
    Backup & restore  http://wiki.apache.org/tajo/BackupAndRestore
    Configuration     http://wiki.apache.org/tajo/Configuration


================
Examples
================

If you want to list all table names, use '\d' meta command as follows: ::

  tajo> \d
  customer
  lineitem
  nation
  orders
  part
  partsupp
  region
  supplier

Now look at the table description: ::

  tajo> \d orders

  table name: orders
  table path: hdfs:/xxx/xxx/tpch/orders
  store type: CSV
  number of rows: 0
  volume (bytes): 172.0 MB
  schema: 
  o_orderkey      INT8
  o_custkey       INT8
  o_orderstatus   TEXT
  o_totalprice    FLOAT8
  o_orderdate     TEXT
  o_orderpriority TEXT
  o_clerk TEXT
  o_shippriority  INT4
  o_comment       TEXT