*****************
How to use index?
*****************

---------------
1. Create index
---------------

The first step for utilizing index is to create an index. You can create an index using SQL (:doc:`/sql_language/ddl`) or Tajo API (:doc:`/tajo_client_api`).
For example, the following SQL statement can be used to create a BST index on the lineitem table.

.. code-block:: sql

     default> create index l_orderkey_idx on lineitem (l_orderkey);

If the index is created successfully, you can see the index information as follows: ::

  default> \d lineitem

  table name: default.lineitem
  table path: hdfs://localhost:7020/tpch/lineitem
  store type: TEXT
  number of rows: unknown
  volume: 753.9 MB
  Options:
  	'text.delimiter'='|'

  schema:
  l_orderkey	INT8
  l_partkey	INT8
  l_suppkey	INT8
  l_linenumber	INT8
  l_quantity	FLOAT4
  l_extendedprice	FLOAT4
  l_discount	FLOAT4
  l_tax	FLOAT4
  l_returnflag	TEXT
  l_linestatus	TEXT
  l_shipdate	DATE
  l_commitdate	DATE
  l_receiptdate	DATE
  l_shipinstruct	TEXT
  l_shipmode	TEXT
  l_comment	TEXT


  Indexes:
  "l_orderkey_idx" TWO_LEVEL_BIN_TREE (l_orderkey ASC NULLS LAST )

For more information about index creation, please refer to the above links.

-----------------------------
2. Enable/disable index scans
-----------------------------

When an index is successfully created, you must enable index scan as follows:

.. code-block:: sql

     default> \set INDEX_ENABLED true

If you don't want to use index scan anymore, you can simply disable it as follows:

.. code-block:: sql

     default> \set INDEX_ENABLED false

.. note::

     Once index scan is enabled, Tajo currently always performs index scan regardless of its efficiency. You should set this option when the expected number of retrieved tuples is sufficiently small.

---------------------------
3. Index backup and restore
---------------------------

Tajo currently provides only the catalog backup and restore for index. Please refer to :doc:`/backup_and_restore/catalog` for more information about catalog backup and restore.