********************************
Add data to Partition Table
********************************

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
    id     INT,
    name   TEXT,
    grade  TEXT
  ) PARTITION BY COLUMN (country TEXT, city TEXT);


================================================
How to INSERT dynamically to partition table
================================================

If you want to load an entire country or an entire city in one fell swoop:

.. code-block:: sql

  INSERT OVERWRITE INTO student
  SELECT id, name, grade, country, city
  FROM   student_source;


================================================
How to CTAS dynamically to partition table
================================================

If you want to load an entire country or an entire city in one fell swoop:

.. code-block:: sql

  DROP TABLE if exists student;

  CREATE TABLE student (
    id     INT,
    name   TEXT,
    grade  TEXT
  ) PARTITION BY COLUMN (country TEXT, city TEXT)
  AS SELECT id, name, grade, country, city
  FROM   student_source;


.. note::

  When loading data into a partition, it’s necessary to include the partition columns as the last columns in the query. The column names in the source query don’t need to match the partition column names.