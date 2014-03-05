*************************
INSERT (OVERWRITE) INTO
*************************

INSERT OVERWRITE statement overwrites a table data of an existing table or a data in a given directory. Tajo's INSERT OVERWRITE statement follows ``INSERT INTO SELECT`` statement of SQL. The examples are as follows:

.. code-block:: sql

  create table t1 (col1 int8, col2 int4, col3 float8);

  -- when a target table schema and output schema are equivalent to each other
  INSERT OVERWRITE INTO t1 SELECT l_orderkey, l_partkey, l_quantity FROM lineitem;
  -- or
  INSERT OVERWRITE INTO t1 SELECT * FROM lineitem;

  -- when the output schema are smaller than the target table schema
  INSERT OVERWRITE INTO t1 SELECT l_orderkey FROM lineitem;

  -- when you want to specify certain target columns
  INSERT OVERWRITE INTO t1 (col1, col3) SELECT l_orderkey, l_quantity FROM lineitem;

In addition, INSERT OVERWRITE statement overwrites table data as well as a specific directory.

.. code-block:: sql

  INSERT OVERWRITE INTO LOCATION '/dir/subdir' SELECT l_orderkey, l_quantity FROM lineitem;