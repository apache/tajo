********************************
Repair partition
********************************

Tajo stores a list of partitions for each table in its catalogstore. If partitions are manually added to the distributed file system, the metastore is not aware of these partitions. Running the ``ALTER TABLE REPAIR PARTITION`` statement ensures that the tables are properly populated.

*Synopsis*

.. code-block:: sql

  ALTER TABLE <table_name> REPAIR PARTITION

*Examples*

.. code-block:: sql

  ALTER TABLE student REPAIR PARTITION;