*********************************
Define Partition Table
*********************************

Tajo makes it easy to specify an automatic partition scheme when the table is created.

================================================
How to Create Partitione Table
================================================

You can create a partitioned table by using the ``PARTITION BY`` clause. For a column partitioned table, you should use
the ``PARTITION BY COLUMN`` clause with partition keys.

For example, assume there is a table ``student`` composed of the following schema.

.. code-block:: sql

  id     INT,
  name   TEXT,
  grade  TEXT

Now you want to partition on country. Your Tajo definition would be this:

.. code-block:: sql

  CREATE TABLE student (
    id     INT,
    name   TEXT,
    grade  TEXT
  ) PARTITION BY COLUMN (country TEXT);

Now your users will still query on ``WHERE country = '...'`` but the 2nd column will be the original values.
Here's an example statement to create a table:

.. code-block:: sql

  CREATE TABLE student (
    id     INT,
    name   TEXT,
    grade  TEXT
  ) USING PARQUET
  PARTITION BY COLUMN (country TEXT, city TEXT);

The statement above creates the student table with id, name, grade. The table is also partitioned and data is stored in parquet files.

You might have noticed that while the partitioning key columns are a part of the table DDL, they’re only listed in the ``PARTITION BY`` clause. In Tajo, as data is written to disk, each partition of data will be automatically split out into different folders, e.g. country=USA/city=NEWYORK. During a read operation, Tajo will use the folder structure to quickly locate the right partitions and also return the partitioning columns as columns in the result set.


==================================================
Partition Pruning on Partition Table
==================================================

The following predicates in the ``WHERE`` clause can be used to prune unqualified column partitions without processing
during query planning phase.

* ``=``
* ``<>``
* ``>``
* ``<``
* ``>=``
* ``<=``
* LIKE predicates with a leading wild-card character
* IN list predicates

Now above example table data is partitioned by country and city, so when the query is applied on table it can easily access the required row by the help partitions


.. code-block:: sql

  SELECT * FROM student WHERE country = 'KOREA' AND city = 'SEOUL';
  SELECT * FROM student WHERE country = 'USA' AND (city = 'NEWYORK' OR city = 'BOSTON');
  SELECT * FROM student WHERE country = 'USA' AND city <> 'NEWYORK';

