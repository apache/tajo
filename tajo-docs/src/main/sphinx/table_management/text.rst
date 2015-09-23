****
TEXT
****

A character-separated values plain text file represents a tabular data set consisting of rows and columns.
Each row is a plain text line. A line is usually broken by a character line feed ``\n`` or carriage-return ``\r``.
The line feed ``\n`` is the default delimiter in Tajo. Each record consists of multiple fields, separated by
some other character or string, most commonly a literal vertical bar ``|``, comma ``,`` or tab ``\t``.
The vertical bar is used as the default field delimiter in Tajo.

============================
How to Create a TEXT Table ?
============================

If you are not familiar with the ``CREATE TABLE`` statement, please refer to the Data Definition Language :doc:`/sql_language/ddl`.

In order to specify a certain file format for your table, you need to use the ``USING`` clause in your ``CREATE TABLE``
statement. The below is an example statement for creating a table using *TEXT* format.

.. code-block:: sql

 CREATE TABLE
  table1 (
    id int,
    name text,
    score float,
    type text
  ) USING TEXT;

===================
Physical Properties
===================

Some table storage formats provide parameters for enabling or disabling features and adjusting physical parameters.
The ``WITH`` clause in the CREATE TABLE statement allows users to set those parameters.

*TEXT* format provides the following physical properties.

* ``text.delimiter``: delimiter character. ``|`` or ``\u0001`` is usually used, and the default field delimiter is ``|``.
* ``text.null``: ``NULL`` character. The default ``NULL`` character is an empty string ``''``. Hive's default ``NULL`` character is ``'\\N'``.
* ``compression.codec``: Compression codec. You can enable compression feature and set specified compression algorithm. The compression algorithm used to compress files. The compression codec name should be the fully qualified class name inherited from `org.apache.hadoop.io.compress.CompressionCodec <https://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/compress/CompressionCodec.html>`_. By default, compression is disabled.
* ``text.serde``: custom (De)serializer class. ``org.apache.tajo.storage.text.CSVLineSerDe`` is the default (De)serializer class.
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
 ) USING TEXT WITH('text.delimiter'='\u0001',
                   'text.null'='\\N',
                   'compression.codec'='org.apache.hadoop.io.compress.SnappyCodec');

.. warning::

  Be careful when using ``\n`` as the field delimiter because *TEXT* format tables use ``\n`` as the line delimiter.
  At the moment, Tajo does not provide a way to specify the line delimiter.

=====================
Custom (De)serializer
=====================

The *TEXT* format not only provides reading and writing interfaces for text data but also allows users to process custom
plain text file formats with user-defined (De)serializer classes.
With custom (de)serializers, Tajo can process any text files no matter which the internal structure is.

In order to specify a custom (De)serializer, set a physical property ``text.serde``.
The property value should be a fully qualified class name.

For example:

.. code-block:: sql

 CREATE TABLE table1 (
  id int,
  name text,
  score float,
  type text
 ) USING TEXT WITH ('text.serde'='org.my.storage.CustomSerializerDeserializer')


==========================
Null Value Handling Issues
==========================
In default, ``NULL`` character in *TEXT* format is an empty string ``''``.
In other words, an empty field is basically recognized as a ``NULL`` value in Tajo.
If a field domain is ``TEXT``, an empty field is recognized as a string value ``''`` instead of ``NULL`` value.
Besides, You can also use your own ``NULL`` character by specifying a physical property ``text.null``.

======================================
Compatibility Issues with Apache Hive™
======================================

*TEXT* tables generated in Tajo can be processed directly by Apache Hive™ without further processing.
In this section, we explain some compatibility issue for users who use both Hive and Tajo.

If you set a custom field delimiter, the *TEXT* tables cannot be directly used in Hive.
In order to specify the custom field delimiter in Hive, you need to use ``ROW FORMAT DELIMITED FIELDS TERMINATED BY``
clause in a Hive's ``CREATE TABLE`` statement as follows:

.. code-block:: sql

 CREATE TABLE table1 (id int, name string, score float, type string)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 STORED AS TEXT

To the best of our knowledge, there is not way to specify a custom ``NULL`` character in Hive.
