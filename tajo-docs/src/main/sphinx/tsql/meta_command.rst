*********************************
Meta Commands
*********************************


In tsql, any command that begins with an unquoted backslash ('\') is a tsql meta-command that is processed by tsql itself.

In the current implementation, there are meta commands as follows: ::

  default> \?


  General
    \copyright    show Apache License 2.0
    \version      show Tajo version
    \?            show help
    \? [COMMAND]  show help of a given command
    \help         alias of \?
    \q            quit tsql


  Informational
    \l           list databases
    \c           show current database
    \c [DBNAME]  connect to new database
    \d           list tables
    \d [TBNAME]  describe table
    \df          list functions
    \df NAME     describe function


  Tool
    \!           execute a linux shell command
    \dfs         execute a dfs command
    \admin       execute Tajo admin command


  Variables
    \set [[NAME] [VALUE]  set session variable or list session variables
    \unset NAME           unset session variable


  Documentations
    tsql guide        http://tajo.apache.org/docs/current/tsql.html
    Query language    http://tajo.apache.org/docs/current/sql_language.html
    Functions         http://tajo.apache.org/docs/current/functions.html
    Backup & restore  http://tajo.apache.org/docs/current/backup_and_restore.html
    Configuration     http://tajo.apache.org/docs/current/configuration.html

-----------------------------------------------
Basic usages
-----------------------------------------------

``\l`` command shows a list of all databases as follows: ::

  default> \l
  default
  tpch
  work1
  default>



``\d`` command shows a list of tables in the current database as follows: ::

  default> \d
  customer
  lineitem
  nation
  orders
  part
  partsupp
  region
  supplier


``\d [table name]`` command also shows a table description as follows: ::

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


``\df`` command shows a list of all built-in functions as follows: ::

  default> \df
   Name            | Result type     | Argument types        | Description                                   | Type
  -----------------+-----------------+-----------------------+-----------------------------------------------+-----------
   abs             | INT4            | INT4                  | Absolute value                                | GENERAL
   abs             | INT8            | INT8                  | Absolute value                                | GENERAL
   abs             | FLOAT4          | FLOAT4                | Absolute value                                | GENERAL
   abs             | FLOAT8          | FLOAT8                | Absolute value                                | GENERAL
   acos            | FLOAT8          | FLOAT4                | Inverse cosine.                               | GENERAL
   acos            | FLOAT8          | FLOAT8                | Inverse cosine.                               | GENERAL
   utc_usec_to     | INT8            | TEXT,INT8             | Extract field from time                       | GENERAL
   utc_usec_to     | INT8            | TEXT,INT8,INT4        | Extract field from time                       | GENERAL

  (181) rows

  For Reference, many details have been omitted in order to present a clear picture of the process.

``\df [function name]`` command also shows a function description as follows: ::

  default> \df round;
   Name            | Result type     | Argument types        | Description                                   | Type
  -----------------+-----------------+-----------------------+-----------------------------------------------+-----------
   round           | INT8            | FLOAT4                | Round to nearest integer.                     | GENERAL
   round           | INT8            | FLOAT8                | Round to nearest integer.                     | GENERAL
   round           | INT8            | INT4                  | Round to nearest integer.                     | GENERAL
   round           | INT8            | INT8                  | Round to nearest integer.                     | GENERAL
   round           | FLOAT8          | FLOAT8,INT4           | Round to s decimalN places.                   | GENERAL
   round           | FLOAT8          | INT8,INT4             | Round to s decimalN places.                   | GENERAL

  (6) rows

  Function:    INT8 round(float4)
  Description: Round to nearest integer.
  Example:
  > SELECT round(42.4)
  42

  Function:    FLOAT8 round(float8,int4)
  Description: Round to s decimalN places.
  Example:
  > SELECT round(42.4382, 2)
  42.44
