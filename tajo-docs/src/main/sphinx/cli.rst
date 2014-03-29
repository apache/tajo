*****************************
Tajo Shell (TSQL)
*****************************

==========
Synopsis
==========

.. code-block:: bash

  bin/tsql [options] [database name]

If a *database_name* is given, tsql connects to the database at startup time. Otherwise, tsql connects to ``default`` database.

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

  default>

If you want to connect a specified TajoMaster, you should use '-h' and (or) 'p' options as follows: ::

  bin/tsql -h localhost -p 9004

  default> 

The prompt indicates the current database.

===================
 Meta Commands
===================

In tsql, anything command that begins with an unquoted backslash ('\') is a tsql meta-command that is processed by tsql itself.

In the current implementation, there are meta commands as follows: ::

  default> \?

  General
    \copyright  show Apache License 2.0
    \version    show Tajo version
    \?          show help
    \q          quit tsql


  Informational
    \l           list databases
    \c           show current database
    \c [DBNAME]  connect to new database
    \d           list tables
    \d [TBNAME]  describe table
    \df          list functions
    \df NAME     describe function


  Variables
    \set [[NAME] [VALUE]  set session variable or list session variables
    \unset NAME           unset session variable


  Documentations
    tsql guide        http://tajo.apache.org/docs/0.8.0/cli.html
    Query language    http://tajo.apache.org/docs/0.8.0/sql_language.html
    Functions         http://tajo.apache.org/docs/0.8.0/functions.html
    Backup & restore  http://tajo.apache.org/docs/0.8.0/backup_and_restore.html
    Configuration     http://tajo.apache.org/docs/0.8.0/configuration.html

-----------------------------------------------
Basic usages
-----------------------------------------------

``\l`` command shows a list of all databases.

.. code-block:: sql

  default> \l
  default
  tpch
  work1
  default> 

``\d`` command shows a list of tables in the current database as follows: ..

  default> \d
  customer
  lineitem
  nation
  orders
  part
  partsupp
  region
  supplier

``\d [table name]`` command also shows a table description.

  default> \d orders

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

The prompt ``default>`` indicates the current database. Basically, all SQL statements and meta commands work in the current database. Also, you can change the current database with ``\c`` command.

.. code-block:: sql

  default> \c work1
  You are now connected to database "test" as user "hyunsik".
  work1>

-----------------------------------------------
Session Variables
-----------------------------------------------

Each client connection to TajoMaster creates a unique session, and the client and TajoMaster uses the session until disconnect. A session provides session variables which are used for various configs per session.

``tsql`` provides the meta command ``\set`` to manipulate session variables. Just ``\set`` command shows all session variables. ::

  default> \set
  'name1'='val1'
  'name2'='val2'
  'name3'='val3'
       ...

``\set key val`` will set the session variable named *key* with the value *val*. ::

  default> \set
  'CURRENT_DATABASE'='default'
  
  default> \set key1 val1

  default> \set
  'CURRENT_DATABASE'='default'
  'key1'='val1'


Also, ``\unset key`` will unset the session variable named *key*.