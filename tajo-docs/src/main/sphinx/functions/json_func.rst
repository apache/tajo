*******************************
JSON Functions
*******************************

.. function:: json_extract_path_text (string json, string xpath)

  Extracts JSON string from a JSON string based on json path specified and returns JSON string pointed to by xPath

  :param string:
  :param string:
  :rtype: text
  :example:

  .. code-block:: sql

    json_extract_path_text('{"test" : {"key" : "tajo"}}','$.test.key');
    > tajo
