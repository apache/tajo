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

    select 'Ta' || 'jo';
    > 'Tajo'
  
.. function:: ascii (string text)

  Returns the ASCII code of the first character of the text.
  For UTF-8, this function returns the Unicode code point of the character.
  For other multibyte encodings, the argument must be an ASCII character.

  :param string: input string
  :rtype: int4
  :example:

  .. code-block:: sql

    select ascii('x');
    > 120

.. function:: bit_length (string text)

  Returns the number of bits in string.

  :param string: input string
  :rtype: int4
  :example:

  .. code-block:: sql

    select bit_length('jose');
    > 32

.. function:: char_length (string text)

  Returns the number of characters in string.

  :param string: to be counted
  :rtype: int4
  :alias: character_length, length
  :example:

  .. code-block:: sql

    select char_length('Tajo');
    > 4

.. function:: octet_length (string text)

  Returns the number of bytes in string.

  :param string: input string
  :rtype: int4
  :example:

  .. code-block:: sql

    select octet_length('jose');
    > 4

.. function:: chr (code int4)

  Returns a character with the given code.

  :param code: input character code
  :rtype: char
  :example:

  .. code-block:: sql

    select chr(65);
    > A

.. function:: decode (binary text, format text)

  Decode binary data from textual representation in string.

  :param binary: encoded value
  :param format: decode format. base64, hex, escape. escape converts zero bytes and high-bit-set bytes to octal sequences (\nnn) and doubles backslashes.
  :rtype: text
  :example:

  .. code-block:: sql

    select decode('MTIzXDAwMFwwMDE=', 'base64');
    > 123\\000\\001

.. function:: digest (input text, method text)

  Calculates the Digest hash of string.

  :param input: input string
  :param method: hash method
  :rtype: text
  :example:

  .. code-block:: sql

    select digest('tajo', 'sha1');
    > 02b0e20540b89f0b735092bbac8093eb2e3804cf

.. function:: encode (binary text, format text)

  Encode binary data into a textual representation.

  :param binary: decoded value
  :param format: encode format. base64, hex, escape. escape converts zero bytes and high-bit-set bytes to octal sequences (\nnn) and doubles backslashes.
  :rtype: text
  :example:

  .. code-block:: sql

    select encode('123\\000\\001', 'base64');
    > MTIzXDAwMFwwMDE=

.. function:: initcap (string text)

  Convert the first letter of each word to upper case  and the rest to lower case.

  :param string: input string
  :rtype: text
  :example:

  .. code-block:: sql

    select initcap('hi THOMAS');
    > Hi Thomas

.. function:: md5 (string text)

  Calculates the MD5 hash of string.

  :param string: input string
  :rtype: text
  :example:

  .. code-block:: sql

    select md5('abc');
    > 900150983cd24fb0d6963f7d28e17f72

.. function:: left (string text, number int4)

  Returns the first n characters in the string.

  :param string: input string
  :param number: number of characters retrieved
  :rtype: text
  :example:

  .. code-block:: sql

    select left('ABC', 2);
    > AB

.. function:: right(string text, number int4)

  Returns the last n characters in the string.

  :param string: input string
  :param number: number of characters retrieved
  :rtype: text
  :example:

  .. code-block:: sql

    select right('ABC', 2);
    > BC

.. function:: locate(source text, target text, [start_index int4])

  Returns the location of specified substring.

  :param source: source string
  :param target: target substring
  :param start_index: the index where the search is started
  :rtype: int4
  :alias: strpos
  :example:

  .. code-block:: sql

    select locate('high', 'ig', 1);
    > 2

.. function:: strposb(source text, target text)

  Returns the binary location of specified substring.

  :param source: source string
  :param target: target substring
  :rtype: int4
  :example:

  .. code-block:: sql

    select strpos('tajo', 'aj');
    > 2

.. function:: substr(source text, start int4, length int4)

  Extract substring.

  :param source: source string
  :param start: start index
  :param length: length of substring
  :rtype: text
  :example:

  .. code-block:: sql

    select substr('alphabet', 3, 2);
    > ph

.. function:: trim(string text, [characters text])

  Removes the characters (a space by default) from the start/end/both ends of the string.

  :param string: input string
  :param characters: characters which will be removed
  :rtype: text
  :example:

  .. code-block:: sql

    select trim('xTajoxx', 'x');
    > Tajo

.. function:: trim([leading | trailing | both] [characters text] FROM string text)

  Removes the characters (a space by default) from the start/end/both ends of the string.

  :param string: input string
  :param characters: characters which will be removed
  :rtype: text
  :example:

  .. code-block:: sql

    select trim(both 'x' from 'xTajoxx');
    > Tajo


.. function:: btrim(string text, [characters text])

  Removes the characters (a space by default) from the both ends of the string.
  
  :param string: input string
  :param characters: characters which will be removed
  :rtype: text
  :alias: trim
  :example:

  .. code-block:: sql

    select btrim('xTajoxx', 'x');
    > Tajo 


.. function:: ltrim(string text, [characters text])

  Removes the characters (a space by default) from the start ends of the string.

  :param string: input string
  :param characters: characters which will be removed
  :rtype: text
  :example:

  .. code-block:: sql

    select ltrim('xxTajo', 'x');
    > Tajo 


.. function:: rtrim(string text, [characters text])

  Removes the characters (a space by default) from the end ends of the string.

  :param string: input string
  :param characters: characters which will be removed
  :rtype: text
  :example:

  .. code-block:: sql

    select rtrim('Tajoxx', 'x');
    > Tajo 


.. function:: split_part(string text, delimiter text, field int)

  Splits a string on delimiter and return the given field (counting from one).

  :param string: input string
  :param delimiter: delimiter
  :param field: index to field
  :rtype: text
  :example:

  .. code-block:: sql

    select split_part('ab_bc_cd','_',2);   
    > bc 



.. function:: regexp_replace(string text, pattern text, replacement text)

  Replaces substrings matched to a given regular expression pattern.

  :param string: input string
  :param pattern: pattern
  :param replacement: string substituted for the matching substring
  :rtype: text
  :example:

  .. code-block:: sql

    select regexp_replace('abcdef', '(ˆab|ef$)', '–'); 
    > –cd–


.. function:: upper(string text)

  Makes an input text to be upper case.

  :param string: input string
  :rtype: text
  :example:

  .. code-block:: sql

    select upper('tajo');
    > TAJO


.. function:: lower(string text)

  Makes an input text to be lower case.

  :param string: input string
  :rtype: text
  :example:

  .. code-block:: sql

    select lower('TAJO');
    > tajo

.. function:: lpad(source text, number int4, pad text)

  Fill up the string to length by prepending the characters fill (a space by default). If the string is already longer than length then it is truncated (on the right).

  :param source: source string
  :param number: padding length
  :param pad: padding string
  :rtype: text
  :example:

  .. code-block:: sql

    select lpad('hi', 5, 'xy');
    > xyxhi

.. function:: rpad(source text, number int4, pad text)

  Fill up the string to length length by appending the characters fill (a space by default). If the string is already longer than length then it is truncated.

  :param source: source string
  :param number: padding length
  :param pad: padding string
  :rtype: text
  :example:

  .. code-block:: sql

    select rpad('hi', 5, 'xy');
    > hixyx

.. function:: quote_ident(string text)

  Return the given string suitably quoted to be used as an identifier in an SQL statement string. Quotes are added only if necessary (i.e., if the string contains non-identifier characters or would be case-folded). Embedded quotes are properly doubled.

  :param string: input string
  :rtype: text
  :example:

  .. code-block:: sql

    select quote_ident('Foo bar');
    > "Foo bar"

.. function:: repeat(string text, number int4)

  Repeat string the specified number of times.

  :param string: input string
  :param number: repetition number
  :rtype: text
  :example:

  .. code-block:: sql

    select repeat('Pg', 4);
    > PgPgPgPg

.. function:: reverse(string text)

  Reverse string.

  :param string: input string
  :rtype: text
  :example:

  .. code-block:: sql

    select reverse('TAJO');
    > OJAT