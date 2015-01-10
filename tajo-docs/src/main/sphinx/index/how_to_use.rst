*************************************
How to use index?
*************************************

-------------------------------------
1. Create index
-------------------------------------

The first step for utilizing index is index creation. You can create index using SQL (:doc:`/sql_language/ddl`) or Tajo API (:doc:`/tajo_client_api`). For example, you can create a BST index on the lineitem table by submitting the following SQL to Tajo.

.. code-block:: sql

     default> create index l_orderkey_idx on lineitem (l_orderkey);

If the index is created successfully, you can see the information about that index as follows: ::

  default> \d lineitem

  table name: default.lineitem
  table path: hdfs://localhost:7020/tpch/lineitem
  store type: CSV
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

-------------------------------------
2. Enable/disable index scans
-------------------------------------

When an index is successfully created, you must enable the index scan feature as follows:

.. code-block:: sql

     default> \set INDEX_ENABLED true

If you don't want to use the index scan feature anymore, you can simply disable it as follows:

.. code-block:: sql

     default> \set INDEX_ENABLED false

.. note::

     Once the index scan feature is enabled, Tajo currently always performs the index scan regardless of its efficiency. You should set this option when the expected number of retrieved tuples is sufficiently small.