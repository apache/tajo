*********************
Aggregation Functions
*********************


.. function:: avg (expression)

  Returns the average of all input values.

  :param expression:
  :type expression: INT* | FLOAT*
  :rtype: FLOAT8

.. function:: corr (expression1, expression2)

  Returns the coefficient of correlation between a set of number pairs.

  :param expression1:
  :param expression2:
  :type expression1: INT* | FLOAT*
  :type expression2: INT* | FLOAT*
  :rtype: FLOAT8

.. function:: count()

  Returns the number of input rows.

  :rtype: INT8

.. function:: last_value(expression)

  Returns the last value of expression.

  :param expression:
  :type expression: INT* | FLOAT* | DATE | TIME | TIMESTAMP | TEXT
  :rtype: same as parameter data type

.. function:: max(expression)

  Returns the maximum value of expression.

  :param expression:
  :type expression: INT* | FLOAT* | DATE | TIME | TIMESTAMP | TEXT
  :rtype: same as parameter data type

.. function:: min(expression)

  Returns the minimum value of expression.

  :param expression:
  :type expression: INT* | FLOAT* | DATE | TIME | TIMESTAMP | TEXT
  :rtype: same as parameter data type

.. function:: stddev_pop(expression)

  Returns the population standard deviation of a set of numbers.

  :param expression:
  :type expression: INT* | FLOAT*
  :rtype: FLOAT8

.. function:: stddev_samp(expression)

  Returns the sample standard deviation of a set of numbers.

  :param expression:
  :type expression: INT* | FLOAT*
  :rtype: FLOAT8

.. function:: sum(expression)

  Returns the sum of a set of numbers.

  :param expression:
  :type expression: INT* | FLOAT*
  :rtype: same as parameter data type

.. function:: var_pop(expression)

  Returns the variance of a set of numbers.

  :param expression:
  :type expression: INT* | FLOAT*
  :rtype: FLOAT8

.. function:: var_samp(expression)

  Returns the unbiased sample variance of a set of numbers.

  :param expression:
  :type expression: INT* | FLOAT*
  :rtype: FLOAT8


.. note:: ``INT*`` means various size of integer types can be accepted. And ``FLOAT*`` means both of ``FLOAT4`` and ``FLOAT8`` are OK.
      For your information, in Tajo SQL, ``INT`` is alias for ``INT4`` and ``FLOAT`` is one for ``FLOAT4``.
      See :doc:`/sql_language/data_model` .