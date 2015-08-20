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

In addition, several functions are provided for type conversion. Please refer to `:doc:data_type_func_and_operators` and `:doc:datetime_func_and_operators`.

-------------------------
String Constants
-------------------------

A string constant is an arbitrary sequence of characters bounded by single quotes (``'``):

.. code-block:: sql

  'tajo'

-------------------------
Function Call
-------------------------

The syntax for a function call consists of the name of a function and its argument list enclosed in parentheses:

.. code-block:: sql

  function_name ([expression [, expression ... ]] )

For more information about functions, please refer to `:doc:functions`.