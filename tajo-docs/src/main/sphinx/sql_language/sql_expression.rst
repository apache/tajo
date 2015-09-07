============================
 SQL Expressions
============================

-------------------------
 Arithmetic Expressions
-------------------------

-------------------------
Type Casts
-------------------------
A type cast converts a specified-typed data to another-typed data. Tajo has two type cast syntax:

.. code-block:: sql

  CAST ( expression AS type )
  expression::type

In addition, several functions are provided for type conversion. Please refer to :doc:`../functions/data_type_func_and_operators` and :doc:`../functions/datetime_func_and_operators`.

-------------------------
String Literals
-------------------------

A string constant is an arbitrary sequence of characters bounded by single quotes (``'``):

.. code-block:: sql

  'tajo'

-------------------------
Function Calls
-------------------------

The syntax for a function call consists of the name of a function and its argument list enclosed in parentheses:

.. code-block:: sql

  function_name ([expression [, expression ... ]] )

For more information about functions, please refer to :doc:`../functions`.

-------------------------
Window Function Calls
-------------------------

A window function call performs aggregate operation across the ``window`` which is a set of rows that are related to the current row. An window function has the following syntax. 

.. code-block:: sql

  function_name OVER ( window_definition )

where *function_name* is the name of aggregation function. Any aggregation function or window function can be used. For built-in aggregation functions and window functions, please refer to :doc:`../functions/agg_func` and :doc:`../functions/window_func`.

*window_definition* has the following syntax.

.. code-block:: sql

  [ PARTITION BY expression [, ...] ]
  [ ORDER BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] ]

In the above syntax, *expression* can be any expression except window function call itself.
*PARTITION BY* and *ORDER BY* lists have the same syntax and semantics as *GROUP BY* and *ORDER BY* clauses.
That is, *PARTITION BY* list describes how the output result will be partitioned like *GROUP BY* clause creates multiple partitions according to the value of its expression.
With *ORDER BY* list, result values are sorted in each partition.

Here are some examples.

.. code-block:: sql

  select l_orderkey, count(*) as cnt, row_number() over (order by count(*) desc) row_num from lineitem group by l_orderkey order by l_orderkey;

  select o_custkey, o_orderstatus, sum(o_totalprice) over (partition by o_custkey) as price from orders;

  select l_linenumber, l_tax, sum(l_quantity) over (partition by l_linenumber order by l_tax desc) as quantity, avg(l_extendedprice) over (partition by l_shipdate) from lineitem order by l_tax;