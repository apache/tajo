*************************************
How to use index?
*************************************

-------------------------------------
1. Create index
-------------------------------------

The first step for utilizing index is index creation. You can create index using SQL (:doc:`/sql_language/ddl`) or Tajo API (:doc:`/tajo_client_api`). For example, you can create a BST index on the lineitem table by submitting the following SQL to Tajo.

.. code-block:: sql

     create index l_orderkey_idx on lineitem (l_orderkey);

For more information, please refer to the above links.

-------------------------------------
2. Enable/disable index scans
-------------------------------------

When an index is successfully created, you must enable the index scan feature as follows.

.. code-block:: sql

     \set INDEX_ENABLED true

.. note::

     If the index scan feature is enabled, Tajo currently always performs index scan regardless of its efficiency. You should use this option when a sufficiently small number of tuples are expected to be retrived.