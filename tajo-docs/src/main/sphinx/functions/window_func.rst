************************************
Window Functions
************************************

.. function:: first_value (value any)

  Returns the first value of input rows.

  :param value: input value
  :rtype: same as parameter data type

.. function:: last_value (value any)

  Returns the last value of input rows.

  :param value: input value
  :rtype: same as parameter data type

.. function:: lag (value any [, offset integer [, default any ]])

  Returns value evaluated at the row that is offset rows before the current row within the partition. If there is no such row, instead return default. Both offset and default are evaluated with respect to the current row. If omitted, offset defaults to 1 and default to null.

  :param value: input value
  :param offset: offset
  :param default: default value
  :rtype: same as parameter data type

.. function:: lead (value any [, offset integer [, default any ]])

  Returns value evaluated at the row that is offset rows after the current row within the partition. If there is no such row, instead return default. Both offset and default are evaluated with respect to the current row. If omitted, offset defaults to 1 and default to null.

  :param value: input value
  :param offset: offset
  :param default: default value
  :rtype: same as parameter data type

.. function:: rank ()

  Returns rank of the current row with gaps.

  :rtype: int8

.. function:: row_number ()

  Returns the current row within its partition, counting from 1.

  :rtype: int8