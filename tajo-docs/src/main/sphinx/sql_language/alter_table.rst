************************
ALTER TABLE Statement
************************

========================
RENAME TABLE
========================

*Synopsis*

.. code-block:: sql

  ALTER TABLE <table_name> RENAME TO <new_table_name>

  For example:
  ALTER TABLE table1 RENAME TO table2;

This statement lets you change the name of a table to a different name.

========================
RENAME COLUMN
========================

*Synopsis*

.. code-block:: sql

  ALTER TABLE <table_name> RENAME COLUMN <column_name> TO <new_column_name>

  For example:
  ALTER TABLE table1 RENAME COLUMN id TO id2;

This statement will allow users to change a column's name.

========================
ADD COLUMN
========================

*Synopsis*

.. code-block:: sql

  ALTER TABLE <table_name> ADD COLUMN <column_name> <data_type>

  For example:
  ALTER TABLE table1 ADD COLUMN id text;

This statement lets you add new columns to the end of the existing column.

========================
SET PROPERTY
========================

*Synopsis*

.. code-block:: sql

  ALTER TABLE <table_name> SET PROPERTY (<key> = <value>, ...)

  For example:
  ALTER TABLE table1 SET PROPERTY 'timezone' = 'GMT-7'
  ALTER TABLE table1 SET PROPERTY 'text.delimiter' = '&'
  ALTER TABLE table1 SET PROPERTY 'compression.type'='RECORD','compression.codec'='org.apache.hadoop.io.compress.SnappyCodec'


This statement will allow users to change a table property.

========================
ADD PARTITION
========================

*Synopsis*

.. code-block:: sql

  ALTER TABLE <table_name> [IF NOT EXISTS] ADD PARTITION (<partition column> = <partition value>, ...) [LOCATION = <partition's path>]

  For example:
  ALTER TABLE table1 ADD PARTITION (col1 = 1 , col2 = 2)
  ALTER TABLE table1 ADD PARTITION (col1 = 1 , col2 = 2) LOCATION 'hdfs://xxx.com/warehouse/table1/col1=1/col2=2'

You can use ``ALTER TABLE ADD PARTITION`` to add partitions to a table. The location must be a directory inside of which data files reside. If the location doesn't exist on the file system, Tajo will make the location by force. ``ADD PARTITION`` changes the table metadata, but does not load data. If the data does not exist in the partition's location, queries will not return any results.

========================
 DROP PARTITION
========================

*Synopsis*

.. code-block:: sql

  ALTER TABLE <table_name> [IF NOT EXISTS] DROP PARTITION (<partition column> = <partition value>, ...) [PURGE]

  For example:
  ALTER TABLE table1 DROP PARTITION (col1 = 1 , col2 = 2)
  ALTER TABLE table1 DROP PARTITION (col1 = '2015' , col2 = '01', col3 = '11' )
  ALTER TABLE table1 DROP PARTITION (col1 = 'TAJO' ) PURGE

You can use ``ALTER TABLE DROP PARTITION`` to drop a partition for a table. This removes the data for a managed table
 and this doesn't remove the data for an external table. But if ``PURGE`` is specified for an external table, the partition data will be removed. The metadata is completely lost in all cases.