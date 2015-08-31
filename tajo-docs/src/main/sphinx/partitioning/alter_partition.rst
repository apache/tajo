********************************
Alter partition
********************************

You can ALTER TABLE to add or drop partitions for partition table.

For example, assume there is a ``student`` table composed of the following schema.

.. code-block:: sql

  CREATE TABLE student (
    id     INT,
    name   TEXT,
    grade  TEXT
  ) PARTITION BY COLUMN (country TEXT, city TEXT);


========================
ADD PARTITION
========================

*Synopsis*

.. code-block:: sql

  ALTER TABLE <table_name> [IF NOT EXISTS] ADD PARTITION (<partition column> = <partition value>, ...) [LOCATION = <partition's path>]

*Description*

You can use ``ALTER TABLE ADD PARTITION`` to ADD PARTITIONs to a table. The location must be a directory inside of which data files reside. If the location doesn't exist on the file system, Tajo will make the location by force. ``ADD PARTITION`` changes the table metadata, but does not load data. If the data does not exist in the partition's location, queries will not return any results. An error is thrown if the partition for the table already exists. You can use ``IF NOT EXISTS`` to skip the error.

*Examples*

.. code-block:: sql

  -- Each ADD PARTITION clause creates a subdirectory in HDFS.
  ALTER TABLE student ADD PARTITION (country='KOREA', city='SEOUL');
  ALTER TABLE student ADD PARTITION (country='KOREA', city='PUSAN');
  ALTER TABLE student ADD PARTITION (country='USA', city='NEWYORK');
  ALTER TABLE student ADD PARTITION (country='USA', city='BOSTON');
  -- Redirect queries, INSERT, and LOAD DATA for one partition to a specific different directory.
  ALTER TABLE student ADD PARTITION (country='USA', city='BOSTON') LOCATION '/usr/external_data/new_years_day';


========================
 DROP PARTITION
========================

*Synopsis*

.. code-block:: sql

  ALTER TABLE <table_name> [IF EXISTS] DROP PARTITION (<partition column> = <partition value>, ...) [PURGE]

*Description*

You can use ``ALTER TABLE DROP PARTITION`` to drop a partition for a table. This doesn't remove the data for partition table. But if ``PURGE`` is specified, the partition data will be removed. The metadata is completely lost in all cases. An error is thrown if the partition for the table doesn't exists. You can use ``IF EXISTS`` to skip the error.

*Examples*

.. code-block:: sql

  -- Delete just metadata
  ALTER TABLE table1 DROP PARTITION (country = 'KOREA' , city = 'SEOUL');
  -- Delete table data and metadata
  ALTER TABLE table1 DROP PARTITION (country = 'USA', city = 'NEWYORK' ) PURGE