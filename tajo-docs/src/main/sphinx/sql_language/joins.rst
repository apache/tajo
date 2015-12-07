**************************
Joins
**************************

=====================
Overview
=====================

In Tajo, a single query can accesses multiple rows of the same or different relations at one time. This query is called *join*.
Currently, Tajo supports cross, inner, and outer joins. 

A join query can involve multiple relations in the ``FROM`` clause according to the following rule.

.. code-block:: sql

  FROM joined_table [, joined_table [, ...] ]

, where ``joined_table`` is:

.. code-block:: sql

  table_reference join_type table_reference [ ON join_condition ]

``join_type`` can be one of the followings.

.. code-block:: sql

  CROSS JOIN
  [ NATURAL ] [ INNER ] JOIN
  { LEFT | RIGHT | FULL } OUTER JOIN

``join_condition`` can be specified in the ``WHERE`` clause as well as the ``ON`` clause.

For more information, please refer to :doc:`/sql_language/predicates`.

.. note::

  Currently, Tajo cannot natively support non-equality conditions. It means that inner joins with non-equality conditions will be executed with cross joins. Outer joins with non-equality conditions cannot be executed yet.

=====================
Examples
=====================

* For inner and outer joins, only equality conditions are allowed as follows. For inner joins, implicit join notation is allowed.

.. code-block:: sql

  SELECT a.* FROM a, b WHERE a.id = b.id

  SELECT a.* FROM a JOIN b ON (a.id = b.id)

  SELECT a.* FROM a LEFT OUTER JOIN b ON (a.id = b.id AND a.type = b.type)

However, the following query will be executed with CROSS join, thereby taking a very long time.

.. code-block:: sql

  SELECT a.* FROM a JOIN b ON (a.id <> b.id)

In addition, the following query is not allowed.

.. code-block:: sql

  SELECT a.* FROM a LEFT OUTER JOIN b ON (a.id > b.id)

* You can join more than 2 tables in a query with multiple join types.

.. code-block:: sql

  SELECT a.* FROM a, b, c WHERE a.id = b.id AND b.id2 = c.id2

  SELECT a.* FROM a INNER JOIN b ON a.id = b.id FULL OUTER JOIN c ON b.id2 = c.id2

When a query involves three or more tables, there may be a lot of possible join orders. Tajo automatically finds the best join order regardless of the input order. For example, suppose that relation ``b`` is larger than relation ``a``, and in turn, the relation ``c`` is larger than relation ``b``. The query

.. code-block:: sql

  SELECT a.* FROM c INNER JOIN b ON b.id2 = c.id2 INNER JOIN a ON a.id = b.id

is rewritten to

.. code-block:: sql

  SELECT a.* FROM a INNER JOIN b ON a.id = b.id INNER JOIN c ON b.id2 = c.id2

because early join of small relations accelerates the query speed.

* Tajo also supports natural join. When relations have a common column name, they are joined with an equality condition on that column even though it is not explicitly declared in the query. For example,

.. code-block:: sql

  SELECT a.* FROM a JOIN b

is rewritten to

.. code-block:: sql

  SELECT a.* FROM a INNER JOIN b ON a.id = b.id

=====================
Join Optimization
=====================

Join is one of the most expensive operations in relational world.
Tajo adopts several optimization techniques to improve its join performance.

---------------------
Join ordering
---------------------

Join ordering is one of the important techniques for join performance improvement.
Basically, joining multiple relations is left-associative. However, query performance can be significantly changed according to which order is chosen for the join execution.

To find the best join order, Tajo's cost-based optimizer considers join conditions, join types, and the size of input relations.
In addition, it considers the computation cost of consecutive joins so that the shape of query plan forms a bushy tree.

For example, suppose that there are 4 relations ``a`` (10), ``b`` (20), ``c`` (30), and ``d`` (40) where the numbers within brackets represent the relation size.
The following query

.. code-block:: sql

  SELECT
    *
  FROM
    a, b, c, d
  WHERE
    a.id1 = b.id1 AND
    a.id4 = d.id4 AND
    b.id2 = c.id2 AND
    c.id3 = d.id3

is rewritten into

.. code-block:: sql

  SELECT
    *
  FROM
    (a INNER JOIN d ON a.id4 = d.id4)
    INNER JOIN
    (b INNER JOIN c ON b.id2 = c.id2)
    ON a.id1 = b.id1 AND c.id3 = d.id3


---------------------
Broadcast join
---------------------

In Tajo, a join query is executed in two stages. The first stage is responsible for scanning input data and performing local join, while the second stage is responsible for performing global join and returning the result.
To perform join globally in the second stage, intermediate result of the first stage is exchanged according to join keys, i.e., *shuffled*, among Tajo workers.
Here, the cost of shuffle is expensive especially when the input relation size is very small.

Broadcast join is a good solution to handle this problem. In broadcast join, the small relations are replicated to every worker who participates in the join computation.
Thus, they can perform join without expensive data shuffle.

Tajo provides a session variable for broadcast join configuration. (For more detailed information of session variables, please refer to :doc:`/tsql/variables`.)

* ``BROADCAST_NON_CROSS_JOIN_THRESHOLD`` and ``BROADCAST_CROSS_JOIN_THRESHOLD`` are thresholds for broadcast join. Only the relations who are larger than this threshold can be broadcasted.

You can also apply this configuration system widely by setting ``tajo.dist-query.broadcast.non-cross-join.threshold-kb`` or ``tajo.dist-query.broadcast.cross-join.threshold-kb`` in ``${TAJO_HOME}/conf/tajo-site.xml``.