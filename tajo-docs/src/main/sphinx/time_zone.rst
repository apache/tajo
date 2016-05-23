******************
Time Zone
******************

Time zone affects ``Timestamp`` data type and operations (e.g., ``to_char``). Tables can have different time zones. Internally, Tajo translates all table rows to UTC values and processes them. It becomes easier for Tajo to handle multiple different time zones.

In Tajo, there are some time zong settings.

========================
Server Cluster Time Zone
========================

One Tajo cluster has a system time zone which globally affects all tables in which the table property 'time zone' are not specified.

You can set the system time zone in *conf/tajo-site.xml* file as follows:

**tajo-site.xml**

.. code-block:: xml  

  <name>tajo.timezone</name>
  <value>GMT+9</value>


==================
Table Time Zone
==================

In Tajo, a table property ``timezone`` allows users to specify a time zone that the table uses for reading or writing. 
When each table row are read or written, ```timestamp``` column values are adjusted by a given time zone if it is set.

You can specify a table time zone as follows:

.. code-block:: sql

   CREATE EXTERNAL TABLE table1 (
    t_timestamp  TIMESTAMP,
    t_date    DATE
   ) USING TEXT WITH('timezone'='ASIA/Seoul') LOCATION '/path-to-table/'
 

In order to learn table properties, please refer to :doc:`/table_management/table_overview`.

==================
Client Time Zone
==================

Each client has its own time zone setting. It translates retrieved timestamp and time values by time zone. In order to set client time zone, you should set the session variable ``TIMEZONE``. There are some ways to set this session variable.

In ``tsql``, you can use ``\set timezone`` meta command as follows:

**tsql**

.. code-block:: sh

  default> \set timezone GMT+9


The following ways use SQL statements. So, this way is available in ``tsql``, JDBC, and Tajo Java API.

**SQL**

.. code-block:: sql

  SET TIME ZONE 'GMT+9';

  or

  SET SESSION TIMEZONE TO 'GMT+9';

============
Time Zone ID
============

Time zone can be an abbreviation form like 'PST' or 'DST'. Also, it accepts an offset-based form like 'GMT+9' or UTC+9' or a location-based form like 'Asia/Seoul'. 

.. note::

  In many cases, offset-based forms or locaion-based forms are recommanded. In order to know the list of time zones, please refer to `List of tz database time zones <http://en.wikipedia.org/wiki/List_of_tz_database_time_zones>`_

.. note::

  Java 6 does not recognize many location-based time zones and an offset-based timezone using the prefix 'UTC'. We highly recommanded using the offset-based time zone using the prefix 'GMT'. In other words, you should use 'GMT-7' instead of 'UTC-7' in Java 6.

=====================
Examples of Time Zone
=====================

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
 ) USING TEXT WITH('text.delimiter'='|', 'timezone'='ASIA/Seoul') LOCATION '/path-to-table/'


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

  default> SET TIME ZONE 'Asia/Seoul'
  default> SELECT * FROM table1
  t_timestamp,            t_date
  ----------------------------------
  1980-04-01 01:50:30.01, 1980-04-01
  1980-04-01 01:50:30   , 1980-04-01
  1980-04-01 01:50:30   , 1980-04-01
