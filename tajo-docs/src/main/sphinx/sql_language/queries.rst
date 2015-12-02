*******
Queries
*******

========
Overview
========

*Synopsis*

.. code-block:: sql

  SELECT [distinct [all]] * | <expression> [[AS] <alias>] [, ...]
    [FROM <table reference> [[AS] <table alias name>] [, ...]]
    [WHERE <condition>]
    [GROUP BY <expression> [, ...]]
    [HAVING <condition>]
    [ORDER BY <expression> [ASC|DESC] [NULLS (FIRST|LAST)] [, ...]]



===========
From Clause
===========

*Synopsis*

.. code-block:: sql

  [FROM <table reference> [[AS] <table alias name>] [, ...]]


The ``FROM`` clause specifies one or more other tables given in a comma-separated table reference list.
A table reference can be a relation name , or a subquery, a table join, or complex combinations of them.

-----------------------
Table and Table Aliases
-----------------------

A temporary name can be given to tables and complex table references to be used
for references to the derived table in the rest of the query. This is called a table alias.

To create a a table alias, please use ``AS``:

.. code-block:: sql

  FROM table_reference AS alias

or

.. code-block:: sql

  FROM table_reference alias

The ``AS`` keyword can be omitted, and *Alias* can be any identifier.

A typical application of table aliases is to give short names to long table references. For example:

.. code-block:: sql

  SELECT * FROM long_table_name_1234 s JOIN another_long_table_name_5678 a ON s.id = a.num;

-------------
Joined Tables
-------------

Tajo supports all kinds of join types.

Join Types
~~~~~~~~~~

Cross Join
^^^^^^^^^^

.. code-block:: sql

  FROM T1 CROSS JOIN T2

Cross join, also called *Cartesian product*, results in every possible combination of rows from T1 and T2.

``FROM T1 CROSS JOIN T2`` is equivalent to ``FROM T1, T2``.

Qualified joins
^^^^^^^^^^^^^^^

Qualified joins implicitly or explicitly have join conditions. Inner/Outer/Natural Joins all are qualified joins.
Except for natural join, ``ON`` or ``USING`` clause in each join is used to specify a join condition. 
A join condition must include at least one boolean expression, and it can also include just filter conditions.

**Inner Join**

.. code-block:: sql

  T1 [INNER] JOIN T2 ON boolean_expression
  T1 [INNER] JOIN T2 USING (join column list)

``INNER`` keyword is the default, and so ``INNER`` can be omitted when you use inner join.

**Outer Join**

.. code-block:: sql

  T1 (LEFT|RIGHT|FULL) OUTER JOIN T2 ON boolean_expression
  T1 (LEFT|RIGHT|FULL) OUTER JOIN T2 USING (join column list)

One of ``LEFT``, ``RIGHT``, or ``FULL`` must be specified for outer joins. 
Join conditions in outer join will have different behavior according to corresponding table references of join conditions.
To know outer join behavior in more detail, please refer to 
`Advanced outer join constructs <http://www.ibm.com/developerworks/data/library/techarticle/purcell/0201purcell.html>`_.

**Natural Join**

.. code-block:: sql

  T1 NATURAL JOIN T2

``NATURAL`` is a short form of ``USING``. It forms a ``USING`` list consisting of all common column names that appear in 
both join tables. These common columns appear only once in the output table. If there are no common columns, 
``NATURAL`` behaves like ``CROSS JOIN``.

**Subqueries**

A subquery is a query that is nested inside another query. It can be embedded in the FROM and WHERE clauses.

Example:

.. code-block:: sql

  FROM (SELECT col1, sum(col2) FROM table1 WHERE col3 > 0 group by col1 order by col1) AS alias_name
  WHERE col1 IN (SELECT col1 FROM table1 WHERE col2 > 0 AND col2 < 100) AS alias_name

For more detailed information, please refer to :doc:`joins`.

============
Where Clause
============

The syntax of the WHERE Clause is

*Synopsis*

.. code-block:: sql

  WHERE search_condition

``search_condition`` can be any boolean expression. 
In order to know additional predicates, please refer to :doc:`/sql_language/predicates`.

==========================
Groupby and Having Clauses
==========================

*Synopsis*

.. code-block:: sql

  SELECT select_list
      FROM ...
      [WHERE ...]
      GROUP BY grouping_column_reference [, grouping_column_reference]...
      [HAVING boolean_expression]

The rows which passes ``WHERE`` filter may be subject to grouping, specified by ``GROUP BY`` clause.
Grouping combines a set of rows having common values into one group, and then computes rows in the group with aggregation functions. ``HAVING`` clause can be used with only ``GROUP BY`` clause. It eliminates the unqualified result rows of grouping.

``grouping_column_reference`` can be a column reference, a complex expression including scalar functions and arithmetic operations.

.. code-block:: sql

  SELECT l_orderkey, SUM(l_quantity) AS quantity FROM lineitem GROUP BY l_orderkey;

  SELECT substr(l_shipdate,1,4) as year, SUM(l_orderkey) AS total2 FROM lineitem GROUP BY substr(l_shipdate,1,4);

If a SQL statement includes ``GROUP BY`` clause, expressions in select list must be either grouping_column_reference or aggregation function. For example, the following example query is not allowed because ``l_orderkey`` does not occur in ``GROUP BY`` clause.

.. code-block:: sql

  SELECT l_orderkey, l_partkey, SUM(l_orderkey) AS total FROM lineitem GROUP BY l_partkey;

Aggregation functions can be used with ``DISTINCT`` keywords. It forces an individual aggregate function to take only distinct values of the argument expression. ``DISTINCT`` keyword is used as follows:

.. code-block:: sql

  SELECT l_partkey, COUNT(distinct l_quantity), SUM(distinct l_extendedprice) AS total FROM lineitem GROUP BY l_partkey;

=========================
Orderby and Limit Clauses
=========================

*Synopsis*

.. code-block:: sql

  FROM ... ORDER BY <sort_expr> [(ASC|DESC)] [NULLS (FIRST|LAST) [,...]

``sort_expr`` can be a column reference, aliased column reference, or a complex expression. 
``ASC`` indicates an ascending order of ``sort_expr`` values. ``DESC`` indicates a descending order of ``sort_expr`` values.
``ASC`` is the default order.

``NULLS FIRST`` and ``NULLS LAST`` options can be used to determine whether nulls values appear 
before or after non-null values in the sort ordering. By default, null values are dealt as if larger than any non-null value; 
that is, ``NULLS FIRST`` is the default for ``DESC`` order, and ``NULLS LAST`` otherwise.

================
Window Functions
================

A window function performs a calculation across multiple table rows that belong to some window frame.

*Synopsis*

.. code-block:: sql

  SELECT ...., func(param) OVER ([PARTITION BY partition-expr [, ...]] [ORDER BY sort-expr [, ...]]), ....,  FROM

The PARTITION BY list within OVER specifies dividing the rows into groups, or partitions, that share the same values of 
the PARTITION BY expression(s). For each row, the window function is computed across the rows that fall into 
the same partition as the current row.

We will briefly explain some examples using window functions.

---------
Examples
---------

Multiple window functions can be used in a SQL statement as follows:

.. code-block:: sql

  SELECT l_orderkey, sum(l_discount) OVER (PARTITION BY l_orderkey), sum(l_quantity) OVER (PARTITION BY l_orderkey) FROM LINEITEM;

If ``OVER()`` clause is empty as following, it makes all table rows into one window frame.

.. code-block:: sql

  SELECT salary, sum(salary) OVER () FROM empsalary;

Also, ``ORDER BY`` clause can be used without ``PARTITION BY`` clause as follows:

.. code-block:: sql

  SELECT salary, sum(salary) OVER (ORDER BY salary) FROM empsalary;

Also, all expressions and aggregation functions are allowed in ``ORDER BY`` clause as follows:

.. code-block:: sql

  select
    l_orderkey,
    count(*) as cnt,
    row_number() over (partition by l_orderkey order by count(*) desc)
    row_num
  from
    lineitem
  group by
    l_orderkey

.. note::

  Currently, Tajo does not support multiple different partition-expressions in one SQL statement.