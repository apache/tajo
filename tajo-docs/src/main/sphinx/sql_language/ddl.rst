************************
Data Definition Language
************************

========================
CREATE DATABASE
========================

*Synopsis*

.. code-block:: sql

  CREATE DATABASE [IF NOT EXISTS] <database_name> 

``IF NOT EXISTS`` allows ``CREATE DATABASE`` statement to avoid an error which occurs when the database exists.

========================
DROP DATABASE
========================

*Synopsis*

.. code-block:: sql

  DROP DATABASE [IF EXISTS] <database_name>

``IF EXISTS`` allows ``DROP DATABASE`` statement to avoid an error which occurs when the database does not exist.

========================
CREATE TABLE
========================

*Synopsis*

.. code-block:: sql

  CREATE TABLE [IF NOT EXISTS] <table_name> [(<column_name> <data_type>, ... )]
  [using <storage_type> [with (<key> = <value>, ...)]] [AS <select_statement>]

  CREATE EXTERNAL TABLE [IF NOT EXISTS] <table_name> (<column_name> <data_type>, ... )
  using <storage_type> [with (<key> = <value>, ...)] LOCATION '<path>'

``IF NOT EXISTS`` allows ``CREATE [EXTERNAL] TABLE`` statement to avoid an error which occurs when the table does not exist.

------------------------
 Compression
------------------------

If you want to add an external table that contains compressed data, you should give 'compression.code' parameter to CREATE TABLE statement.

.. code-block:: sql

  create EXTERNAL table lineitem (
  L_ORDERKEY bigint, 
  L_PARTKEY bigint, 
  ...
  L_COMMENT text) 

  USING TEXT WITH ('text.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec')
  LOCATION 'hdfs://localhost:9010/tajo/warehouse/lineitem_100_snappy';

`compression.codec` parameter can have one of the following compression codecs:
  * org.apache.hadoop.io.compress.BZip2Codec
  * org.apache.hadoop.io.compress.DeflateCodec
  * org.apache.hadoop.io.compress.GzipCodec
  * org.apache.hadoop.io.compress.SnappyCodec 

========================
 DROP TABLE
========================

*Synopsis*

.. code-block:: sql

  DROP TABLE [IF EXISTS] <table_name> [PURGE]

``IF EXISTS`` allows ``DROP DATABASE`` statement to avoid an error which occurs when the database does not exist. ``DROP TABLE`` statement removes a table from Tajo catalog, but it does not remove the contents. If ``PURGE`` option is given, ``DROP TABLE`` statement will eliminate the entry in the catalog as well as the contents.

========================
 CREATE INDEX
========================

*Synopsis*

.. code-block:: sql

  CREATE INDEX [ name ] ON table_name [ USING method ]
  ( { column_name | ( expression ) } [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] )
  [ WHERE predicate ]

------------------------
 Index method
------------------------

Currently, Tajo supports only one type of index.

Index methods:
  * TWO_LEVEL_BIN_TREE: This method is used by default in Tajo. For more information about its structure, please refer to :doc:`/index/types`.

========================
 DROP INDEX
========================

*Synopsis*

.. code-block:: sql

  DROP INDEX name