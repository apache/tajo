*******************************
JSON Functions
*******************************

.. function:: json_extract_path_text (string json, string json_path)

  Extracts JSON string from a JSON string based on json path specified and returns JSON string pointed to by JSONPath.
  Returns null if either argument is null.

  :param json: JSON string
  :param json_path: JSONpath
  :rtype: text
  :example:

  .. code-block:: sql

    select json_extract_path_text('{"test" : {"key" : "tajo"}}','$.test.key');
    > tajo

.. function:: json_array_get (string json_array, int index)

  Returns the element at the specified index into the JSON array. This function returns an element indexed from the end of an array with a negative index, and null if the element at the specified index doesn’t exist.

  :param json_array: String of a JSON array
  :param index: index
  :rtype: text
  :example:

  .. code-block:: sql

    select json_array_get('[100, 200, 300]', 0);
    > 100

    select json_array_get('[100, 200, 300]', -2);
    > 200

.. function:: json_array_contains (string json_array, any value)

  Determine if the given value exists in the JSON array.

  :param json_array: String of a JSON array
  :param value: value of any type
  :rtype: text
  :example:

  .. code-block:: sql

    select json_array_contains('[100, 200, 300]', 100);
    > t

.. function:: json_array_length(string json_array)

  Returns the length of json array.

  :param json_array: String of a JSON array
  :rtype: int8
  :example:

  .. code-block:: sql

    select json_array_length('[100, 200, 300]');
    > 3
