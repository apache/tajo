******************
Table Management
******************

In Tajo, a table is a logical view of one data sources. Logically, one table consists of a logical schema, partitions, URL, and various properties. Physically, A table can be a directory in HDFS, a single file, one HBase table, or a RDBMS table. In order to make good use of Tajo, users need to understand features and physical characteristics of their physical layout. This section explains all about table management.

.. toctree::
    :maxdepth: 1

    table_management/table_overview
    table_management/tablespaces
    table_management/file_formats
    table_management/compression