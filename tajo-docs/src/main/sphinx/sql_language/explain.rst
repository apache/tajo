************************
EXPLAIN
************************

*Synopsis*

.. code-block:: sql

  EXPLAIN [GLOBAL] statement


*Description*

Show the logical or global execution plan of a statement.


*Examples*

Logical plan:

.. code-block:: sql

  default> EXPLAIN SELECT l_orderkey, count(*) FROM lineitem GROUP BY l_orderkey;
  explain
  -------------------------------
  GROUP_BY(1)(l_orderkey)
    => exprs: (count())
    => target list: default.lineitem.l_orderkey (INT8), ?count (INT8)
    => out schema:{(2) default.lineitem.l_orderkey (INT8), ?count (INT8)}
    => in schema:{(1) default.lineitem.l_orderkey (INT8)}
     SCAN(0) on default.lineitem
       => target list: default.lineitem.l_orderkey (INT8)
       => out schema: {(1) default.lineitem.l_orderkey (INT8)}
       => in schema: {(16) default.lineitem.l_orderkey (INT8), default.lineitem.l_partkey (INT8), default.lineitem.l_suppkey (INT8), default.lineitem.l_linenumber (INT8), default.lineitem.l_quantity (FLOAT8), default.lineitem.l_extendedprice (FLOAT8), default.lineitem.l_discount (FLOAT8), default.lineitem.l_tax (FLOAT8), default.lineitem.l_returnflag (TEXT), default.lineitem.l_linestatus (TEXT), default.lineitem.l_shipdate (DATE), default.lineitem.l_commitdate (DATE), default.lineitem.l_receiptdate (DATE), default.lineitem.l_shipinstruct (TEXT), default.lineitem.l_shipmode (TEXT), default.lineitem.l_comment (TEXT)}


Global plan:

.. code-block:: sql

  default> EXPLAIN GLOBAL SELECT l_orderkey, count(*) FROM lineitem GROUP BY l_orderkey;
  explain
  -------------------------------
  -------------------------------------------------------------------------------
  Execution Block Graph (TERMINAL - eb_0000000000000_0000_000003)
  -------------------------------------------------------------------------------
  |-eb_0000000000000_0000_000003
     |-eb_0000000000000_0000_000002
        |-eb_0000000000000_0000_000001
  -------------------------------------------------------------------------------
  Order of Execution
  -------------------------------------------------------------------------------
  1: eb_0000000000000_0000_000001
  2: eb_0000000000000_0000_000002
  3: eb_0000000000000_0000_000003
  -------------------------------------------------------------------------------

  =======================================================
  Block Id: eb_0000000000000_0000_000001 [LEAF]
  =======================================================

  [Outgoing]
  [q_0000000000000_0000] 1 => 2 (type=HASH_SHUFFLE, key=default.lineitem.l_orderkey (INT8), num=32)

  GROUP_BY(5)(l_orderkey)
    => exprs: (count())
    => target list: default.lineitem.l_orderkey (INT8), ?count_1 (INT8)
    => out schema:{(2) default.lineitem.l_orderkey (INT8), ?count_1 (INT8)}
    => in schema:{(1) default.lineitem.l_orderkey (INT8)}
     SCAN(0) on default.lineitem
       => target list: default.lineitem.l_orderkey (INT8)
       => out schema: {(1) default.lineitem.l_orderkey (INT8)}
       => in schema: {(16) default.lineitem.l_orderkey (INT8), default.lineitem.l_partkey (INT8), default.lineitem.l_suppkey (INT8), default.lineitem.l_linenumber (INT8), default.lineitem.l_quantity (FLOAT8), default.lineitem.l_extendedprice (FLOAT8), default.lineitem.l_discount (FLOAT8), default.lineitem.l_tax (FLOAT8), default.lineitem.l_returnflag (TEXT), default.lineitem.l_linestatus (TEXT), default.lineitem.l_shipdate (DATE), default.lineitem.l_commitdate (DATE), default.lineitem.l_receiptdate (DATE), default.lineitem.l_shipinstruct (TEXT), default.lineitem.l_shipmode (TEXT), default.lineitem.l_comment (TEXT)}

  =======================================================
  Block Id: eb_0000000000000_0000_000002 [ROOT]
  =======================================================

  [Incoming]
  [q_0000000000000_0000] 1 => 2 (type=HASH_SHUFFLE, key=default.lineitem.l_orderkey (INT8), num=32)

  GROUP_BY(1)(l_orderkey)
    => exprs: (count(?count_1 (INT8)))
    => target list: default.lineitem.l_orderkey (INT8), ?count (INT8)
    => out schema:{(2) default.lineitem.l_orderkey (INT8), ?count (INT8)}
    => in schema:{(2) default.lineitem.l_orderkey (INT8), ?count_1 (INT8)}
     SCAN(6) on eb_0000000000000_0000_000001
       => out schema: {(2) default.lineitem.l_orderkey (INT8), ?count_1 (INT8)}
       => in schema: {(2) default.lineitem.l_orderkey (INT8), ?count_1 (INT8)}

  =======================================================
  Block Id: eb_0000000000000_0000_000003 [TERMINAL]
  =======================================================