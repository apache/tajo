*************************************
Overview of Tajo Tables
*************************************

Overview
========

Managed Table
================

.. todo::

External Table
================

.. todo::

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
