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

.. function:: to_char(source int8, format text)

  Convert integer to string.

  :param source: source value
  :param format: format
  :rtype: text
  :example:

  .. code-block:: sql

    select to_char(125, '00999');
    > 00125

.. function:: to_hex(source int4)

  Convert the argument to hexadecimal.

  :param source: source value
  :rtype: text
  :example:

  .. code-block:: sql

    select to_hex(15);
    > F
