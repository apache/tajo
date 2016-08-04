*********************************
Data Type Functions and Operators
*********************************

.. function:: to_bin(source INT4)

  Returns the binary representation of integer.

  :param source: source value
  :rtype: TEXT
  :example:

  .. code-block:: sql

    select to_bin(22);
    > 10110

.. function:: to_char(source INT8, format TEXT)

  Convert integer to string.

  :param source: source value
  :param format: format
  :rtype: TEXT
  :example:

  .. code-block:: sql

    select to_char(125, '00999');
    > 00125

.. function:: to_hex(source INT4)

  Convert the argument to hexadecimal.

  :param source: source value
  :rtype: TEXT
  :example:

  .. code-block:: sql

    select to_hex(15);
    > F
