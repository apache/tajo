*********************************
Column Partitioning
*********************************

The column table partition is designed to support the partition of Apache Hive™.

================================================
How to Create a Column Partitioned Table
================================================

You can create a partitioned table by using the ``PARTITION BY`` clause. For a column partitioned table, you should use
the ``PARTITION BY COLUMN`` clause with partition keys.

For example, assume there is a table ``orders`` composed of the following schema. ::

  id          INT,
  item_name   TEXT,
  price       FLOAT

Also, assume that you want to use ``order_date TEXT`` and ``ship_date TEXT`` as the partition keys.
Then, you should create a table as follows:

.. code-block:: sql

  CREATE TABLE orders (
    id INT,
    item_name TEXT,
    price
  ) PARTITION BY COLUMN (order_date TEXT, ship_date TEXT);

==================================================
Partition Pruning on Column Partitioned Tables
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

==================================================
Compatibility Issues with Apache Hive™
==================================================

If partitioned tables of Hive are created as external tables in Tajo, Tajo can process the Hive partitioned tables directly.
There haven't known compatibility issues yet.