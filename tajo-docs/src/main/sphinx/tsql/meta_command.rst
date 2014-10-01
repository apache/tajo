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
    tsql guide        http://tajo.apache.org/docs/current/cli.html
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
   add_days        | TIMESTAMP       | DATE,INT2             | Return date value which is added with given pa| GENERAL
   add_days        | TIMESTAMP       | DATE,INT4             | Return date value which is added with given pa| GENERAL
   add_days        | TIMESTAMP       | DATE,INT8             | Return date value which is added with given pa| GENERAL
   add_days        | TIMESTAMP       | TIMESTAMP,INT2        | Return date value which is added with given pa| GENERAL
   add_days        | TIMESTAMP       | TIMESTAMP,INT4        | Return date value which is added with given pa| GENERAL
   add_days        | TIMESTAMP       | TIMESTAMP,INT8        | Return date value which is added with given pa| GENERAL
   add_months      | TIMESTAMP       | DATE,INT2             | Return date value which is added with given pa| GENERAL
   add_months      | TIMESTAMP       | DATE,INT4             | Return date value which is added with given pa| GENERAL
   add_months      | TIMESTAMP       | DATE,INT8             | Return date value which is added with given pa| GENERAL
   add_months      | TIMESTAMP       | TIMESTAMP,INT2        | Return date value which is added with given pa| GENERAL
   add_months      | TIMESTAMP       | TIMESTAMP,INT4        | Return date value which is added with given pa| GENERAL
   add_months      | TIMESTAMP       | TIMESTAMP,INT8        | Return date value which is added with given pa| GENERAL
   ascii           | INT4            | TEXT                  | ASCII code of the first character of the argum| GENERAL
   asin            | FLOAT8          | FLOAT4                | Inverse sine.                                 | GENERAL
   asin            | FLOAT8          | FLOAT8                | Inverse sine.                                 | GENERAL
   atan            | FLOAT8          | FLOAT4                | Inverse tangent.                              | GENERAL
   atan            | FLOAT8          | FLOAT8                | Inverse tangent.                              | GENERAL
   atan2           | FLOAT8          | FLOAT4,FLOAT4         | Inverse tangent of y/x.                       | GENERAL
   atan2           | FLOAT8          | FLOAT8,FLOAT8         | Inverse tangent of y/x.                       | GENERAL
   avg             | FLOAT8          | INT8                  | the mean of a set of numbers                  | AGGREGATIO
   avg             | FLOAT8          | FLOAT4                | The mean of a set of numbers.                 | AGGREGATIO
   avg             | FLOAT8          | INT4                  | the mean of a set of numbers.                 | AGGREGATIO
   avg             | FLOAT8          | FLOAT8                | The mean of a set of numbers.                 | AGGREGATIO
   bit_length      | INT4            | TEXT                  | Number of bits in string                      | GENERAL
   btrim           | TEXT            | TEXT                  |  Remove the longest string consisting only of | GENERAL
   btrim           | TEXT            | TEXT,TEXT             |  Remove the longest string consisting only of | GENERAL
   cbrt            | FLOAT8          | FLOAT4                | Cube root                                     | GENERAL
   cbrt            | FLOAT8          | FLOAT8                | Cube root                                     | GENERAL
   ceil            | INT8            | FLOAT4                | Smallest integer not less than argument.      | GENERAL
   ceil            | INT8            | FLOAT8                | Smallest integer not less than argument.      | GENERAL
   ceiling         | INT8            | FLOAT4                | Smallest integer not less than argument.      | GENERAL
   ceiling         | INT8            | FLOAT8                | Smallest integer not less than argument.      | GENERAL
   char_length     | INT4            | TEXT                  | Number of characters in string                | GENERAL
   character_length| INT4            | TEXT                  | Number of characters in string                | GENERAL
   chr             | CHAR            | INT4                  | Character with the given code.                | GENERAL
   coalesce        | BOOLEAN         | BOOLEAN,BOOLEAN_ARRAY | Returns the first of its arguments that is not| GENERAL
   coalesce        | INT8            | INT8_ARRAY            | Returns the first of its arguments that is not| GENERAL
   coalesce        | FLOAT8          | FLOAT8_ARRAY          | Returns the first of its arguments that is not| GENERAL
   coalesce        | TEXT            | TEXT_ARRAY            | Returns the first of its arguments that is not| GENERAL
   coalesce        | DATE            | DATE_ARRAY            | Returns the first of its arguments that is not| GENERAL
   coalesce        | TIME            | TIME_ARRAY            | Returns the first of its arguments that is not| GENERAL
   coalesce        | TIMESTAMP       | TIMESTAMP_ARRAY       | Returns the first of its arguments that is not| GENERAL
   concat          | TEXT            | TEXT,TEXT_ARRAY       | Concatenate all arguments.                    | GENERAL
   concat_ws       | TEXT            | TEXT,TEXT,TEXT_ARRAY  | Concatenate all but first arguments with separ| GENERAL
   cos             | FLOAT8          | FLOAT4                | Cosine.                                       | GENERAL
   cos             | FLOAT8          | FLOAT8                | Cosine.                                       | GENERAL
   count           | INT8            | ANY                   |  The number of rows for which the supplied exp| DISTINCT_A
   count           | INT8            |                       | the total number of retrieved rows            | AGGREGATIO
   count           | INT8            | ANY                   | The number of retrieved rows for which the sup| AGGREGATIO
   current_date    | DATE            |                       | Get current date. Result is DATE type.        | GENERAL
   current_time    | TIME            |                       | Get current time. Result is TIME type.        | GENERAL
   date            | INT8            | INT4                  | Extracts the date part of the date or datetime| GENERAL
   date_part       | FLOAT8          | TEXT,TIME             | Extract field from time                       | GENERAL
   date_part       | FLOAT8          | TEXT,TIMESTAMP        | Extract field from timestamp                  | GENERAL
   date_part       | FLOAT8          | TEXT,DATE             | Extract field from date                       | GENERAL
   decode          | TEXT            | TEXT,TEXT             | Decode binary data from textual representation| GENERAL
   degrees         | FLOAT8          | FLOAT4                | Radians to degrees                            | GENERAL
   degrees         | FLOAT8          | FLOAT8                | Radians to degrees                            | GENERAL
   digest          | TEXT            | TEXT,TEXT             | Calculates the Digest hash of string          | GENERAL
   div             | INT8            | INT8,INT8             | Division(integer division truncates results)  | GENERAL
   div             | INT8            | INT8,INT4             | Division(integer division truncates results)  | GENERAL
   div             | INT8            | INT4,INT8             | Division(integer division truncates results)  | GENERAL
   div             | INT8            | INT4,INT4             | Division(integer division truncates results)  | GENERAL
   encode          | TEXT            | TEXT,TEXT             | Encode binary data into a textual representati| GENERAL
   exp             | FLOAT8          | FLOAT4                | Exponential                                   | GENERAL
   exp             | FLOAT8          | FLOAT8                | Exponential                                   | GENERAL
   find_in_set     | INT4            | TEXT,TEXT             | Returns the first occurrence of str in str_arr| GENERAL
   floor           | INT8            | FLOAT4                |  Largest integer not greater than argument.   | GENERAL
   floor           | INT8            | FLOAT8                |  Largest integer not greater than argument.   | GENERAL
   geoip_country_co| TEXT            | TEXT                  | Convert an ipv4 address string to a geoip coun| GENERAL
   geoip_country_co| TEXT            | INET4                 | Convert an ipv4 address to a geoip country cod| GENERAL
   geoip_in_country| BOOLEAN         | TEXT,TEXT             | If the given country code is same with the cou| GENERAL
   geoip_in_country| BOOLEAN         | INET4,TEXT            | If the given country code is same with the cou| GENERAL
   initcap         | TEXT            | TEXT                  | Convert the first letter of each word to upper| GENERAL
   left            | TEXT            | TEXT,INT4             | First n characters in the string.             | GENERAL
   length          | INT4            | TEXT                  | Number of characters in string.               | GENERAL
   locate          | INT4            | TEXT,TEXT             | Location of specified substring               | GENERAL
   locate          | INT4            | TEXT,TEXT,INT4        | Location of specified substring               | GENERAL
   lower           | TEXT            | TEXT                  | Convert string to lower case                  | GENERAL
   lpad            | TEXT            | TEXT,INT4             | Fill up the string to length length by prepend| GENERAL
   lpad            | TEXT            | TEXT,INT4,TEXT        | Fill up the string to length length by prepend| GENERAL
   ltrim           | TEXT            | TEXT                  | Remove the longest string containing only char| GENERAL
   ltrim           | TEXT            | TEXT,TEXT             | Remove the longest string containing only char| GENERAL
   max             | INT4            | INT4                  | the maximum value of expr                     | AGGREGATIO
   max             | INT8            | INT8                  | the maximum value of expr                     | AGGREGATIO
   max             | FLOAT4          | FLOAT4                | the maximum value of expr                     | AGGREGATIO
   max             | FLOAT8          | FLOAT8                | the maximum value of expr                     | AGGREGATIO
   max             | TEXT            | TEXT                  | the maximum value of expr                     | AGGREGATIO
   md5             | TEXT            | TEXT                  | Calculates the MD5 hash of string             | GENERAL
   min             | INT4            | INT4                  | the minimum value of expr                     | AGGREGATIO
   min             | INT8            | INT8                  | the minimum value of expr                     | AGGREGATIO
   min             | FLOAT4          | FLOAT4                | the minimum value of expr                     | AGGREGATIO
   min             | FLOAT8          | FLOAT8                | the minimum value of expr                     | AGGREGATIO
   min             | TEXT            | TEXT                  | the minimum value of expr                     | AGGREGATIO
   mod             | INT8            | INT8,INT8             | Remainder of y/x                              | GENERAL
   mod             | INT8            | INT8,INT4             | Remainder of y/x                              | GENERAL
   mod             | INT8            | INT4,INT8             | Remainder of y/x                              | GENERAL
   mod             | INT8            | INT4,INT4             | Remainder of y/x                              | GENERAL
   now             | TIMESTAMP       |                       | Get current time. Result is TIMESTAMP type.   | GENERAL
   octet_length    | INT4            | TEXT                  | Number of bytes in string.                    | GENERAL
   pi              | FLOAT8          |                       | "??" constant                                  | GENERAL
   pow             | FLOAT8          | FLOAT4,FLOAT4         | x raised to the power of y                    | GENERAL
   pow             | FLOAT8          | FLOAT4,FLOAT8         | x raised to the power of y                    | GENERAL
   pow             | FLOAT8          | FLOAT8,FLOAT4         | x raised to the power of y                    | GENERAL
   pow             | FLOAT8          | FLOAT8,FLOAT8         | x raised to the power of y                    | GENERAL
   pow             | FLOAT8          | INT4,INT4             | x raised to the power of y                    | GENERAL
   pow             | FLOAT8          | INT4,INT8             | x raised to the power of y                    | GENERAL
   pow             | FLOAT8          | INT8,INT4             | x raised to the power of y                    | GENERAL
   pow             | FLOAT8          | INT8,INT8             | x raised to the power of y                    | GENERAL
   pow             | FLOAT8          | INT4,FLOAT4           | x raised to the power of y                    | GENERAL
   pow             | FLOAT8          | INT4,FLOAT8           | x raised to the power of y                    | GENERAL
   pow             | FLOAT8          | INT8,FLOAT4           | x raised to the power of y                    | GENERAL
   pow             | FLOAT8          | INT8,FLOAT8           | x raised to the power of y                    | GENERAL
   pow             | FLOAT8          | FLOAT4,INT4           | x raised to the power of y                    | GENERAL
   pow             | FLOAT8          | FLOAT4,INT8           | x raised to the power of y                    | GENERAL
   pow             | FLOAT8          | FLOAT8,INT4           | x raised to the power of y                    | GENERAL
   pow             | FLOAT8          | FLOAT8,INT8           | x raised to the power of y                    | GENERAL
   quote_ident     | TEXT            | TEXT                  | Return the given string suitably quoted to be | GENERAL
   radians         | FLOAT8          | FLOAT8                | Degrees to radians                            | GENERAL
   radians         | FLOAT8          | FLOAT4                | Degrees to radians                            | GENERAL
   random          | INT4            | INT4                  | A pseudorandom number                         | GENERAL
   rank            | INT8            |                       |  The number of rows for which the supplied exp| WINDOW
   regexp_replace  | TEXT            | TEXT,TEXT,TEXT        |  Replace substring(s) matching a POSIX regular| GENERAL
   repeat          | TEXT            | TEXT,INT4             | Repeat string the specified number of times.  | GENERAL
   reverse         | TEXT            | TEXT                  | Reverse str                                   | GENERAL
   right           | TEXT            | TEXT,INT4             | Last n characters in the string               | GENERAL
   round           | INT8            | FLOAT4                | Round to nearest integer.                     | GENERAL
   round           | INT8            | FLOAT8                | Round to nearest integer.                     | GENERAL
   round           | INT8            | INT4                  | Round to nearest integer.                     | GENERAL
   round           | INT8            | INT8                  | Round to nearest integer.                     | GENERAL
   round           | FLOAT8          | FLOAT8,INT4           | Round to s decimalN places.                   | GENERAL
   round           | FLOAT8          | INT8,INT4             | Round to s decimalN places.                   | GENERAL
   row_number      | INT8            |                       | the total number of retrieved rows            | WINDOW
   rpad            | TEXT            | TEXT,INT4             | Fill up the string to length length by appendi| GENERAL
   rpad            | TEXT            | TEXT,INT4,TEXT        | Fill up the string to length length by appendi| GENERAL
   rtrim           | TEXT            | TEXT                  | Remove the longest string containing only  cha| GENERAL
   rtrim           | TEXT            | TEXT,TEXT             | Remove the longest string containing only  cha| GENERAL
   sign            | FLOAT8          | FLOAT8                | sign of the argument (-1, 0, +1)              | GENERAL
   sign            | FLOAT8          | FLOAT4                | sign of the argument (-1, 0, +1)              | GENERAL
   sign            | FLOAT8          | INT8                  | sign of the argument (-1, 0, +1)              | GENERAL
   sign            | FLOAT8          | INT4                  | sign of the argument (-1, 0, +1)              | GENERAL
   sin             | FLOAT8          | FLOAT4                | Sine.                                         | GENERAL
   sin             | FLOAT8          | FLOAT8                | Sine.                                         | GENERAL
   sleep           | INT4            | INT4                  | sleep for seconds                             | GENERAL
   split_part      | TEXT            | TEXT,TEXT,INT4        | Split string on delimiter and return the given| GENERAL
   sqrt            | FLOAT8          | FLOAT8                | Square root                                   | GENERAL
   sqrt            | FLOAT8          | FLOAT4                | Square root                                   | GENERAL
   strpos          | INT4            | TEXT,TEXT             | Location of specified substring.              | GENERAL
   strposb         | INT4            | TEXT,TEXT             | Binary location of specified substring.       | GENERAL
   substr          | TEXT            | TEXT,INT4             | Extract substring.                            | GENERAL
   substr          | TEXT            | TEXT,INT4,INT4        | Extract substring.                            | GENERAL
   sum             | INT8            | INT8                  | the sum of a distinct and non-null values     | DISTINCT_A
   sum             | INT8            | INT8                  | the sum of a set of numbers                   | AGGREGATIO
   sum             | INT8            | INT4                  | the sum of a set of numbers                   | AGGREGATIO
   sum             | INT8            | INT4                  | the sum of a distinct and non-null values     | DISTINCT_A
   sum             | FLOAT8          | FLOAT8                | the sum of a set of numbers                   | AGGREGATIO
   sum             | FLOAT8          | FLOAT4                | the sum of a set of numbers                   | AGGREGATIO
   sum             | FLOAT8          | FLOAT4                | the sum of a distinct and non-null values     | DISTINCT_A
   sum             | FLOAT8          | FLOAT8                | the sum of a distinct and non-null values     | DISTINCT_A
   tan             | FLOAT8          | FLOAT4                | Tangent.                                      | GENERAL
   tan             | FLOAT8          | FLOAT8                | Tangent.                                      | GENERAL
   to_bin          | TEXT            | INT8                  | Returns n in binary.                          | GENERAL
   to_bin          | TEXT            | INT4                  | Returns n in binary.                          | GENERAL
   to_char         | TEXT            | TIMESTAMP,TEXT        | Convert time stamp to string. Format should be| GENERAL
   to_date         | DATE            | TEXT,TEXT             | Convert string to date. Format should be a SQL| GENERAL
   to_hex          | TEXT            | INT4                  | Convert the argument to hexadecimal           | GENERAL
   to_hex          | TEXT            | INT8                  | Convert the argument to hexadecimal           | GENERAL
   to_timestamp    | TIMESTAMP       | TEXT,TEXT             | Convert string to time stamp                  | GENERAL
   to_timestamp    | TIMESTAMP       | INT4                  | Convert UNIX epoch to time stamp              | GENERAL
   to_timestamp    | TIMESTAMP       | INT8                  | Convert UNIX epoch to time stamp              | GENERAL
   trim            | TEXT            | TEXT                  |  Remove the longest string consisting only of | GENERAL
   trim            | TEXT            | TEXT,TEXT             |  Remove the longest string consisting only of | GENERAL
   upper           | TEXT            | TEXT                  | Convert string to upper case.                 | GENERAL
   utc_usec_to     | INT8            | TEXT,INT8             | Extract field from time                       | GENERAL
   utc_usec_to     | INT8            | TEXT,INT8,INT4        | Extract field from time                       | GENERAL

  (181) rows


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
