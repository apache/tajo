************************
Data Definition Language
************************


========================
CREATE TABLE
========================

*Synopsis*

.. code-block:: sql

  CREATE TABLE <table_name> [(<column_name> <data_type>, ... )]
  [using <storage_type> [with (<key> = <value>, ...)]] [AS <select_statement>]

  CREATE EXTERNAL TABLE

  CREATE EXTERNAL TABLE <table_name> (<column_name> <data_type>, ... )
  using <storage_type> [with (<key> = <value>, ...)] LOCATION '<path>'



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

  USING csv WITH ('csvfile.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec')
  LOCATION 'hdfs://localhost:9010/tajo/warehouse/lineitem_100_snappy';

`compression.codec` parameter can have one of the following compression codecs:
  * org.apache.hadoop.io.compress.BZip2Codec
  * org.apache.hadoop.io.compress.DeflateCodec
  * org.apache.hadoop.io.compress.GzipCodec
  * org.apache.hadoop.io.compress.SnappyCodec 

========================
 DROP TABLE
========================

.. code-block:: sql

  DROP TABLE <table_name>