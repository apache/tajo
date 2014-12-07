*************************************
Overview of Tajo Tables
*************************************

Overview
========

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
 ) USING CSV WITH('text.delimiter'='\u0001',
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
When each table row are read or written, ```timestamp``` and ```time``` column values are adjusted by a given time zone if it is set. Time zone can be an abbreviation form like 'PST' or 'DST'. Also, it accepts an offset-based form like 'UTC+9' or a location-based form like 'Asia/Seoul'. 

Each table has one time zone, and many tables can have different time zones. Internally, Tajo translates all tables data to UTC-based values. So, complex queries like join with multiple time zones work well.

.. note::

  In many cases, offset-based forms or locaion-based forms are recommanded. In order to know the list of time zones, please refer to `List of tz database time zones <http://en.wikipedia.org/wiki/List_of_tz_database_time_zones>`_

How time zone works in Tajo
^^^^^^^^^^^^^^^^^^^^^^^^^^^

For example, consider that there is a list of delimited text lines where each rows are written with ``Asia/Seoul`` time zone (i.e., GMT + 9).

.. code-block:: text

  1980-4-1 01:50:30.010|1980-04-01
  80/4/1 1:50:30 AM|80/4/1
  1980 April 1 1:50:30|1980-04-01


In order to register the table, we should put a table property ``'timezone'='Asia/Seoul'`` in ``CREATE TABLE`` statement as follows:

.. code-block:: sql

 CREATE EXTERNAL TABLE table1 (
  t_timestamp  TIMESTAMP,
  t_date    DATE
 ) USING TEXTFILE WITH('text.delimiter'='|', 'timezone'='ASIA/Seoul') LOCATION '/path-to-table/'


By default, ``tsql`` and ``TajoClient`` API use UTC time zone. So, timestamp values in the result are adjusted by the time zone offset. But, date is not adjusted because date type does not consider time zone.

.. code-block:: sql

  default> SELECT * FROM table1
  t_timestamp,            t_date
  ----------------------------------
  1980-03-31 16:50:30.01, 1980-04-01
  1980-03-31 16:50:30   , 1980-04-01
  1980-03-31 16:50:30   , 1980-04-01

In addition, users can set client-side time zone by setting a session variable 'TZ'. It enables a client to translate timestamp or time values to user's time zoned ones.

.. code-block:: sql

  default> \set TZ 'Asia/Seoul'  
  default> SELECT * FROM table1
  t_timestamp,            t_date
  ----------------------------------
  1980-04-01 01:50:30.01, 1980-04-01
  1980-04-01 01:50:30   , 1980-04-01
  1980-04-01 01:50:30   , 1980-04-01