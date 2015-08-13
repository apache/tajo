************************************
Data Type Functions and Operators
************************************

.. function:: to_bin(source int4)

  Returns the binary representation of integer.

  :param source: source value
  :rtype: text
  :example:

  .. code-block:: sql

    select to_bin(22);
    > 10110

.. function:: to_char(source timestamp, format text)

  Convert timestamp to string. Format should be a SQL standard format string.

  :param source: source value
  :param format: format
  :rtype: text
  :example:

  .. code-block:: sql

    select to_char(TIMESTAMP '2014-01-17 10:09:37', 'YYYY-MM');
    > 2014-01

.. function:: to_char(source int8, format text)

  Convert integer to string.

  :param source: source value
  :param format: format
  :rtype: text
  :example:

  .. code-block:: sql

    select to_char(125, '00999');
    > 00125

.. function:: to_date(source text, format text)

  Convert string to date. Format should be a SQL standard format string.

  :param source: source value
  :param format: format
  :rtype: text
  :example:

  .. code-block:: sql

    select to_date('2014-01-01', 'YYYY-MM-DD');
    > 2014-01-01

.. function:: to_hex(source int4)

  Convert the argument to hexadecimal.

  :param source: source value
  :rtype: text
  :example:

  .. code-block:: sql

    select to_hex(15);
    > F

.. function:: to_timestamp(source text, format text)

  Convert string to time stamp. Patterns for Date/Time Formatting: http://www.postgresql.org/docs/8.4/static/functions-formatting.html

  :param source: source value
  :param format: format
  :rtype: text
  :example:

  .. code-block:: sql

    select to_timestamp('05 Dec 2000 15:12:02.020', 'DD Mon YYYY HH24:MI:SS.MS');
    > 2000-12-05 15:12:02.02