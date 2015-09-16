****
JSON
****

JSON(JavaScript Object Notation)

============================
How to Create a JSON Table ?
============================

You can create JSON tables using ``CREATE TABLE`` statement. For more information, please refer to :doc:`/sql_language/ddl`.

.. code-block:: bash
  $ hdfs dfs -cat /table1/table.json
  { "title" : "Hand of the King", "name" : { "first_name": "Eddard", "last_name": "Stark"}}
  { "title" : "Assassin", "name" : { "first_name": "Arya", "last_name": "Stark"}}
  { "title" : "Dancing Master", "name" : { "first_name": "Syrio", "last_name": "Forel"}}




In order to specify a certain file format for your table, you need to use the ``USING`` clause in your ``CREATE TABLE``
statement. The below is an example of creating a table of JSON format.

.. code-block:: sql

  CREATE TABLE table1 (
    id int,
    name text,
    score float,
    type text
  ) USING JSON;

In addition, there is an alternative way to create JSON table.
When you create an external table of JSON format, you can simply omit the column definition as follows:

.. code-block:: sql

  CREATE EXTERNAL TABLE table1 USING JSON LOCATION '/path/to/table1';



===================
Physical Properties
===================

* ``text.delimiter``: delimiter character. ``|`` or ``\u0001`` is usually used, and the default field delimiter is ``|``.
* ``text.null``: ``NULL`` character. The default ``NULL`` character is an empty string ``''``. Hive's default ``NULL`` character is ``'\\N'``.
* ``compression.codec``: Compression codec. You can enable compression feature and set specified compression algorithm. The compression algorithm used to compress files. The compression codec name should be the fully qualified class name inherited from `org.apache.hadoop.io.compress.CompressionCodec <https://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/compress/CompressionCodec.html>`_. By default, compression is disabled.
* ``timezone``: the time zone that the table uses for writting. When table rows are read or written, ```timestamp``` and ```time``` column values are adjusted by this timezone if it is set. Time zone can be an abbreviation form like 'PST' or 'DST'. Also, it accepts an offset-based form like 'UTC+9' or a location-based form like 'Asia/Seoul'.
* ``text.error-tolerance.max-num``: the maximum number of permissible parsing errors. This value should be an integer value. By default, ``text.error-tolerance.max-num`` is ``0``. According to the value, parsing errors will be handled in different ways.

  * If ``text.error-tolerance.max-num < 0``, all parsing errors are ignored.
  * If ``text.error-tolerance.max-num == 0``, any parsing error is not allowed. If any error occurs, the query will be failed. (default)
  * If ``text.error-tolerance.max-num > 0``, the given number of parsing errors in each task will be pemissible.

* ``text.skip.headerlines``: Number of header lines to be skipped. Some text files often have a header which has a kind of metadata(e.g.: column names), thus this option can be useful.

The following example is to set a custom field delimiter, ``NULL`` character, and compression codec:

.. code-block:: sql

  CREATE TABLE table1 (
    id int,
    name text,
    score float,
    type text
  ) USING JSON WITH('text.delimiter'='\u0001',
                    'text.null'='\\N',
                    'compression.codec'='org.apache.hadoop.io.compress.SnappyCodec');

