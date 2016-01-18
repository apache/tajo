*********************************
Column Partitioning
*********************************

The column table partition is designed to support the partition of Apache Hive™.

================================================
How to Create a Column Partitioned Table
================================================

You can create a partitioned table by using the ``PARTITION BY`` clause. For a column partitioned table, you should use
the ``PARTITION BY COLUMN`` clause with partition keys.

For example, assume a table with the following schema.

.. code-block:: sql

  id        INT,
  name      TEXT,
  gender    char(1),
  grade     TEXT,
  country   TEXT,
  city      TEXT,
  phone     TEXT
  );

If you want to make country as partitioned column, your Tajo definition would be this:

.. code-block:: sql

  CREATE TABLE student (
    id        INT,
    name      TEXT,
    gender    char(1),
    grade     TEXT,
    city      TEXT,
    phone     TEXT
  ) PARTITION BY COLUMN (country TEXT);

Let us assume you want to use more partition columns and parquet file format. Here's an example statement to create a table:

.. code-block:: sql

  CREATE TABLE student (
    id        INT,
    name      TEXT,
    gender    char(1),
    grade     TEXT,
    phone     TEXT
  ) USING PARQUET
  PARTITION BY COLUMN (country TEXT, city TEXT);

The statement above creates the student table with id, name, grade, etc. The table is also partitioned and data is stored in parquet files.

You might have noticed that while the partitioning key columns are a part of the table DDL, they’re only listed in the ``PARTITION BY`` clause. In Tajo, as data is written to disk, each partition of data will be automatically split out into different folders, e.g. country=USA/city=NEWYORK. During a read operation, Tajo will use the folder structure to quickly locate the right partitions and also return the partitioning columns as columns in the result set.


==================================================
Querying Partitioned Tables
==================================================

If a table created using the ``PARTITION BY`` clause, a query can do partition pruning and scan only a fraction of the table relevant to the partitions specified by the query. Tajo currently does partition pruning if the partition predicates are specified in the WHERE clause. For example, if table student is partitioned on column country and column city, the following query retrieves rows in ``country=KOREA\city=SEOUL`` directory.

.. code-block:: sql

  SELECT * FROM student WHERE country = 'KOREA' AND city = 'SEOUL';

The following predicates in the ``WHERE`` clause can be used to prune column partitions during query planning phase.

* ``=``
* ``<>``
* ``>``
* ``<``
* ``>=``
* ``<=``
* LIKE predicates with a leading wild-card character
* IN list predicates


==================================================
Add data to Partition Table
==================================================

Tajo provides a very useful feature of dynamic partitioning. You don't need to use any syntax with both ``INSERT INTO ... SELECT`` and ``Create Table As Select(CTAS)`` statments for dynamic partitioning. Tajo will automatically filter the data, create directories, move filtered data to appropriate directory and create partition over it.

For example, assume there are both ``student_source`` and ``student`` tables composed of the following schema.

.. code-block:: sql

  CREATE TABLE student_source (
    id        INT,
    name      TEXT,
    gender    char(1),
    grade     TEXT,
    country   TEXT,
    city      TEXT,
    phone     TEXT
  );

  CREATE TABLE student (
    id        INT,
    name      TEXT,
    gender    char(1),
    grade     TEXT,
    phone     TEXT
  ) PARTITION BY COLUMN (country TEXT, city TEXT);


How to INSERT dynamically to partition table
--------------------------------------------------------

If you want to load an entire country or an entire city in one fell swoop:

.. code-block:: sql

  INSERT OVERWRITE INTO student
  SELECT id, name, gender, grade, phone, country, city
  FROM   student_source;


How to CTAS dynamically to partition table
--------------------------------------------------------

when a partition table is created:

.. code-block:: sql

  DROP TABLE if exists student;

  CREATE TABLE student (
    id        INT,
    name      TEXT,
    gender    char(1),
    grade     TEXT,
    phone     TEXT
  ) PARTITION BY COLUMN (country TEXT, city TEXT)
  AS SELECT id, name, gender, grade, phone, country, city
  FROM   student_source;


.. note::

  When loading data into a partition, it’s necessary to include the partition columns as the last columns in the query. The column names in the source query don’t need to match the partition column names.


==================================================
Compatibility Issues with Apache Hive™
==================================================

If partitioned tables of Hive are created as external tables in Tajo, Tajo can process the Hive partitioned tables directly.


How to create partition table
--------------------------------------------------------

If you create a partition table as follows in Tajo:

.. code-block:: sql

  default> CREATE TABLE student (
    id        INT,
    name      TEXT,
    gender    char(1),
    grade     TEXT,
    phone     TEXT
  ) PARTITION BY COLUMN (country TEXT, city TEXT);


And then you can get table information in Hive:

.. code-block:: sql

  hive> desc student;
  OK
  id                  	int
  name                	string
  gender              	char(1)
  grade               	string
  phone               	string
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
    gender char(1),
    grade string,
    phone string
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
  gender	CHAR(1)
  grade	TEXT
  phone	TEXT

  Partitions:
  type:COLUMN
  columns::default.student.country (TEXT), default.student.city (TEXT)


How to add data to partition table
--------------------------------------------------------

In Tajo, you can add data dynamically to partition table of Hive with both ``INSERT INTO ... SELECT`` and ``Create Table As Select (CTAS)`` statments. Tajo will automatically filter the data to HiveMetastore, create directories and move filtered data to appropriate directory on the distributed file system.

