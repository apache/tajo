***********
 Predicates
***********

=============
 IN Predicate
=============

IN predicate provides a comparison of row, array, and result of a subquery.

*Synopsis*

.. code-block:: sql

  column_reference (NOT) IN (val1, val2, ..., valN)
  column_reference (NOT) IN (SELECT ... FROM ...) AS alias_name


Examples are as follows:

.. code-block:: sql

  -- this statement filters lists down all the records where col1 value is 1, 2 or 3:
  SELECT col1, col2 FROM table1 WHERE col1 IN (1, 2, 3);

  -- this statement filters lists down all the records where col1 value is neither 1, 2 nor 3:
  SELECT col1, col2 FROM table1 WHERE col1 NOT IN (1, 2, 3);

You can use `IN clause` on text data domain as follows:

.. code-block:: sql

  SELECT col1, col2 FROM table1 WHERE col2 IN ('tajo', 'hadoop');

  SELECT col1, col2 FROM table1 WHERE col2 NOT IN ('tajo', 'hadoop');

Finally, you can use subqueries in the `IN clause`.

.. code-block:: sql

  SELECT col1, col2
  FROM table1
  WHERE col3 IN (
    SELECT avg(col2) as avg_col2
    FROM table2
    GROUP BY col1
    HAVING avg_col2 > 100);

  SELECT col1, col2
  FROM table1
  WHERE col3 NOT IN (
    SELECT avg(col2) as avg_col2
    FROM table2
    GROUP BY col1
    HAVING avg_col2 > 100);

==================================
String Pattern Matching Predicates
==================================

--------------------
LIKE
--------------------

LIKE operator returns true or false depending on whether its pattern matches the given string. An underscore (_) in pattern matches any single character. A percent sign (%) matches any sequence of zero or more characters.

*Synopsis*

.. code-block:: sql

  string LIKE pattern
  string NOT LIKE pattern


--------------------
ILIKE
--------------------

ILIKE is the same to LIKE, but it is a case insensitive operator. It is not in the SQL standard. We borrow this operator from PostgreSQL.

*Synopsis*

.. code-block:: sql

  string ILIKE pattern
  string NOT ILIKE pattern


--------------------
SIMILAR TO
--------------------

*Synopsis*

.. code-block:: sql

  string SIMILAR TO pattern
  string NOT SIMILAR TO pattern

It returns true or false depending on whether its pattern matches the given string. Also like LIKE, ``SIMILAR TO`` uses ``_`` and ``%`` as metacharacters denoting any single character and any string, respectively.

In addition to these metacharacters borrowed from LIKE, 'SIMILAR TO' supports more powerful pattern-matching metacharacters borrowed from regular expressions:

+------------------------+-------------------------------------------------------------------------------------------+
| metacharacter          | description                                                                               |
+========================+===========================================================================================+
| &#124;                 | denotes alternation (either of two alternatives).                                         |
+------------------------+-------------------------------------------------------------------------------------------+
| *                      | denotes repetition of the previous item zero or more times.                               |
+------------------------+-------------------------------------------------------------------------------------------+
| +                      | denotes repetition of the previous item one or more times.                                |
+------------------------+-------------------------------------------------------------------------------------------+
| ?                      | denotes repetition of the previous item zero or one time.                                 |
+------------------------+-------------------------------------------------------------------------------------------+
| {m}                    | denotes repetition of the previous item exactly m times.                                  |
+------------------------+-------------------------------------------------------------------------------------------+
| {m,}                   | denotes repetition of the previous item m or more times.                                  |
+------------------------+-------------------------------------------------------------------------------------------+
| {m,n}                  | denotes repetition of the previous item at least m and not more than n times.             |
+------------------------+-------------------------------------------------------------------------------------------+
| []                     | A bracket expression specifies a character class, just as in POSIX regular expressions.   |
+------------------------+-------------------------------------------------------------------------------------------+
| ()                     | Parentheses can be used to group items into a single logical item.                        |
+------------------------+-------------------------------------------------------------------------------------------+

Note that `.`` is not used as a metacharacter in ``SIMILAR TO`` operator.

---------------------
Regular expressions
---------------------

Regular expressions provide a very powerful means for string pattern matching. In the current Tajo, regular expressions are based on Java-style regular expressions instead of POSIX regular expression. The main difference between java-style one and POSIX's one is character class.

*Synopsis*

.. code-block:: sql

  string ~ pattern
  string !~ pattern

  string ~* pattern
  string !~* pattern

+----------+---------------------------------------------------------------------------------------------------+
| operator | Description                                                                                       |
+==========+===================================================================================================+
| ~        | It returns true if a given regular expression is matched to string. Otherwise, it returns false.  |
+----------+---------------------------------------------------------------------------------------------------+
| !~       | It returns false if a given regular expression is matched to string. Otherwise, it returns true.  |
+----------+---------------------------------------------------------------------------------------------------+
| ~*       | It is the same to '~', but it is case insensitive.                                                |
+----------+---------------------------------------------------------------------------------------------------+
| !~*      | It is the same to '!~', but it is case insensitive.                                               |
+----------+---------------------------------------------------------------------------------------------------+

Here are examples:

.. code-block:: sql

  'abc'   ~   '.*c'               true
  'abc'   ~   'c'                 false
  'aaabc' ~   '([a-z]){3}bc       true
  'abc'   ~*  '.*C'               true
  'abc'   !~* 'B.*'               true

Regular expressions operator is not in the SQL standard. We borrow this operator from PostgreSQL.

*Synopsis for REGEXP and RLIKE operators*

.. code-block:: sql

  string REGEXP pattern
  string NOT REGEXP pattern

  string RLIKE pattern
  string NOT RLIKE pattern

But, they do not support case-insensitive operators.