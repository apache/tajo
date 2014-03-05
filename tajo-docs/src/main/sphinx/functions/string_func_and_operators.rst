*******************************
String Functions and Operators
*******************************

.. function:: str1 || str2

  Returns the concatnenated string of both side strings ``str1`` and ``str2``.

  :param str1: first string
  :param str2: second string
  :rtype: text
  :example:

  .. code-block:: sql

    select ‘Ta’ || ‘jo’; 
    > 'Tajo'
  

.. function:: char_length (string text)

  Returns Number of characters in string

  :param string: to be counted
  :rtype: int4
  :alias: character_length
  :example:

  .. code-block:: sql

    select char_length(‘Tajo’);
    > 4


.. function:: trim([leading | trailing | both] [characters] from string)

  Removes the characters (a space by default) from the start/end/both ends of the string

  :param string: 
  :param characters: 
  :rtype: text
  :example:

  .. code-block:: sql

    select trim(both ‘x’ from ‘xTajoxx’);
    > Tajo   


.. function:: btrim(string text, [characters text])

  Removes the characters (a space by default) from the both ends of the string
  
  :param string: 
  :param characters: 
  :rtype: text
  :alias: trim
  :example:

  .. code-block:: sql

    select btrim(‘xTajoxx’, ‘x’);
    > Tajo 


.. function:: ltrim(string text, [characters text])

  Removes the characters (a space by default) from the start ends of the string

  :param string: 
  :param characters: 
  :rtype: text
  :example:

  .. code-block:: sql

    select ltrim(‘xxTajo’, ‘x’);
    > Tajo 


.. function:: rtrim(string text, [characters text])

  Removes the characters (a space by default) from the end ends of the string

  :param string: 
  :param characters: 
  :rtype: text
  :example:

  .. code-block:: sql

    select rtrim('Tajoxx', 'x');
    > Tajo 


.. function:: split_part(string text, delimiter text, field int)

  Splits a string on delimiter and return the given field (counting from one)

  :param string: 
  :param delimiter: 
  :param field: 
  :rtype: text
  :example:

  .. code-block:: sql

    select split_part(‘ab_bc_cd’,‘_’,2);   
    > bc 



.. function:: regexp_replace(string text, pattern text, replacement text)

  Replaces substrings matched to a given regular expression pattern

  :param string: 
  :param pattern: 
  :param replacement: 
  :rtype: text
  :example:

  .. code-block:: sql

    select regexp_replace(‘abcdef’, ‘(ˆab|ef$)’, ‘–’); 
    > –cd–


.. function:: upper(string text)

  makes an input text to be upper case

  :param string:
  :rtype: text
  :example:

  .. code-block:: sql

    select upper('tajo');
    > TAJO


.. function:: lower(string text)

  makes an input text to be lower case

  :param string:
  :rtype: text
  :example:

  .. code-block:: sql

    select lower('TAJO');
    > tajo
