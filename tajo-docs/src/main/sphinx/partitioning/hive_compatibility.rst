********************************
Hive Compatibility
********************************

Tajo provides HiveCatalogStore to process the Hive partitioned tables directly. If you wish to use HiveCatalogStore, you should specify hive configurations to both tajo-env.sh file and catalog-site.xml file. Please see the following page.

.. toctree::
    :maxdepth: 1

   /hive_integration

================================================
How to create partition table
================================================

If you want to create a partition table as follows in Tajo:

.. code-block:: sql

  default> CREATE TABLE student (
    id     INT,
    name   TEXT,
    grade  TEXT
  ) PARTITION BY COLUMN (country TEXT, city TEXT);


And then you can get table information in Hive:

.. code-block:: sql

  hive> desc student;
  OK
  id                  	int
  name                	string
  grade               	string
  country             	string
  city                	string

  # Partition Information
  # col_name            	data_type           	comment

  country             	string
  city                	string


Or as you create the table in Hive:

.. code-block:: sql

  hive > CREATE TABLE student (
    id int,
    name string,
    grade string
  ) PARTITIONED BY (country string, city string)
  ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|' ;

You will see table information in Tajo:

.. code-block:: sql

  default> \d student;
  table name: default.student
  table uri: hdfs://your_hdfs_namespace/user/hive/warehouse/student
  store type: TEXT
  number of rows: 0
  volume: 0 B
  Options:
    'text.null'='\\N'
    'transient_lastDdlTime'='1438756422'
    'text.delimiter'='|'

  schema:
  id	INT4
  name	TEXT
  grade	TEXT

  Partitions:
  type:COLUMN
  columns::default.student.country (TEXT), default.student.city (TEXT)



================================================
How to add data to partition table
================================================

In Tajo, you can add data dynamically to partition table of Hive with both ``INSERT INTO ... SELECT`` and ``Create Table As Select (CTAS)`` statments. Tajo will automatically filter the data to HiveMetastore, create directories and move filtered data to appropriate directory on the distributed file system


