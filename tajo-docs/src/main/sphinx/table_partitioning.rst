******************
Table Partitioning
******************

Table partitioning provides two benefits: easy table management and data pruning by partition keys. Apache Tajo provides the column table partition which is supported for multiple columns in a table. In Tajo, partitioning is supported for both managed and external tables in the table definition. And the column table partition is designed to support the partition of Apache Hive™.

.. toctree::
    :maxdepth: 1

    partitioning/define_partition_table
    partitioning/add_data_to_partition_table
    partitioning/alter_partition
    partitioning/repair_partition
    partitioning/hive_compatibility