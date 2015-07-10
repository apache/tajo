*************************************
Overview of Tajo Tables
*************************************

Overview
========

Tablespaces
===========

Tablespaces is a physical location where files or data objects representing data rows can be stored. Once defined, a tablespace can be referred to by a name when creating a database or a table. Especially, it is very useful when a Tajo cluster instance should use heterogeneous storage systems such as HDFS, MySQL, and Oracle because each tablespace can be specified to use a different storage system. 

Please refer to :doc:`/table_management/tablespaces` if you want to know more information about tablespaces.

Managed Table
================

``CREATE TABLE`` statement with ``EXTERNAL`` keyword lets you create a table located in the warehouse directory specified by the configuration property ``tajo.warehouse.directory`` or ``${tajo.root}/warehouse`` by default. For example: 

.. code-block:: sql

 CREATE TABLE employee (
  id int,
  name text,
  age
 );


External Table
================

``CREATE EXTERNAL TABLE`` statement lets you create a table located in a specify location so that Tajo does not use a default data warehouse location for the table. External tables are in common used if you already have data generated. LOCATION clause must be required for an external table. 

.. code-block:: sql

 CREATE EXTERNAL TABLE employee (
  id int,
  name text,
  age
 ) LOCATION 'hdfs://table/path';


The location can be a directory located in HDFS, Amazon S3, HBase, or local file system (if a Tajo cluster runs in a single machine). URI examples are as follows:

 * HDFS - ``hdfs://hostname:8020/table1``
 * Amazon S3 - ``s3://bucket-name/table1``
 * local file system - ``file:///dir/table1``
 * Openstack Swift - ``swift://bucket-name/table1``

Table Properties
================
All table formats provide parameters for enabling or disabling features and adjusting physical parameters.
The ``WITH`` clause in the CREATE TABLE statement allows users to set those properties.

The following example is to set a custom field delimiter, NULL character, and compression codec:

.. code-block:: sql

 CREATE TABLE table1 (
  id int,
  name text,
  score float,
  type text
 ) USING TEXT WITH('text.delimiter'='\u0001',
                   'text.null'='\\N',
                   'compression.codec'='org.apache.hadoop.io.compress.SnappyCodec');

Each physical table layout has its own specialized properties. They will be addressed in :doc:`/table_management/file_formats`.


Common Table Properties
=======================

There are some common table properties which are used in most tables.

Compression
-----------
.. todo::

Time zone
---------

In Tajo, a table property ``timezone`` allows users to specify a time zone that the table uses for reading or writing. 

You can specify a table time zone as follows:

.. code-block:: sql

   CREATE EXTERNAL TABLE table1 (
    t_timestamp  TIMESTAMP,
    t_date    DATE
   ) USING TEXT WITH('timezone'='ASIA/Seoul') LOCATION '/path-to-table/'
 

In order to learn time zone, please refer to :doc:`/time_zone`.
