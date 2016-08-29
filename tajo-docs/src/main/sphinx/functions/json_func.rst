**************
JSON Functions
**************

.. function:: json_extract_path_text (json TEXT, json_path TEXT)

  Extracts JSON string from a JSON string based on json path specified and returns JSON string pointed to by JSONPath.
  Returns null if either argument is null.

  :param json: JSON string
  :param json_path: JSON path
  :rtype: TEXT
  :example:

  .. code-block:: sql

    select json_extract_path_text('{"test" : {"key" : "tajo"}}','$.test.key');
    > tajo

.. function:: json_array_get (json_array TEXT, index INT4)

  Returns the element at the specified index into the JSON array. This function returns an element indexed from the end of an array with a negative index, and null if the element at the specified index doesn’t exist.

  :param json_array: String of a JSON array
  :param index: index
  :rtype: TEXT
  :example:

  .. code-block:: sql

    select json_array_get('[100, 200, 300]', 0);
    > 100

    select json_array_get('[100, 200, 300]', -2);
    > 200

.. function:: json_array_contains (json_array TEXT, value ANY)

  Determine if the given value exists in the JSON array.

  :param json_array: String of a JSON array
  :param value: value of any type
  :rtype: TEXT
  :example:

  .. code-block:: sql

    select json_array_contains('[100, 200, 300]', 100);
    > t

.. function:: json_array_length(json_array TEXT)

  Returns the length of json array.

  :param json_array: String of a JSON array
  :rtype: INT8
  :example:

  .. code-block:: sql

    select json_array_length('[100, 200, 300]');
    > 3
