****
JSON
****

JSON(JavaScript Object Notation) is an open standard format for data (de)serialization. Since it is simple and human-readable, it is popularly used in many fields.
Tajo supports JSON as its data format. In this section, you will get an overview of how to create JSON tables and query on them.

============================
How to Create a JSON Table ?
============================

You can create a JSON table using the ``CREATE TABLE`` statement. (For more information, please refer to :doc:`/sql_language/ddl`.)
For example, please consider an example data as follows:

.. code-block:: bash

  $ hdfs dfs -cat /table1/table.json
  { "title" : "Hand of the King", "name" : { "first_name": "Eddard", "last_name": "Stark"}}
  { "title" : "Assassin", "name" : { "first_name": "Arya", "last_name": "Stark"}}
  { "title" : "Dancing Master", "name" : { "first_name": "Syrio", "last_name": "Forel"}}

Tajo provides two ways to create a table for this data. First is a traditional way to create tables. Here is an example.

.. code-block:: sql

  CREATE EXTERNAL TABLE table1 (
    title TEXT,
    name RECORD (
      first_name TEXT,
      last_name TEXT
    )
  ) USING JSON LOCATION '/table1/table.json';

With this way, you need to specify every column which they want to use. This will be a tedious work, and not appropriate for flexible JSON schema.
Second is a simpler alternative to alleviate this problem. When you create an external table of JSON format, you can simply omit the column specification as follows:

.. code-block:: sql

  CREATE EXTERNAL TABLE table1 (*) USING JSON LOCATION '/table1/table.json';

No matter which way you choose, you can submit any queries on this table.

.. code-block:: sql

  > SELECT title, name.last_name from table1 where name.first_name = 'Arya';
  title,name/last_name
  -------------------------------
  Assassin,Stark

.. warning::

  If you create a table with the second way, every column is assumed as the ``TEXT`` type.
  So, you need to perform type casting if you want to handle them as other types.

===================
Physical Properties
===================

Some table storage formats provide parameters for enabling or disabling features and adjusting physical parameters.
The ``WITH`` clause in the CREATE TABLE statement allows users to set those parameters.

The JSON format provides the following physical properties.

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

.. warning::

  Be careful when using ``\n`` as the field delimiter because *TEXT* format tables use ``\n`` as the line delimiter.
  At the moment, Tajo does not provide a way to specify the line delimiter.

==========================
Null Value Handling Issues
==========================
In default, ``NULL`` character in *TEXT* format is an empty string ``''``.
In other words, an empty field is basically recognized as a ``NULL`` value in Tajo.
If a field domain is ``TEXT``, an empty field is recognized as a string value ``''`` instead of ``NULL`` value.
Besides, You can also use your own ``NULL`` character by specifying a physical property ``text.null``.
