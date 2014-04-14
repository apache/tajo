*************************************
RCFile
*************************************

RCFile, short of Record Columnar File, are flat files consisting of binary key/value pairs,
which shares many similarities with SequenceFile.

=========================================
How to Create a RCFile Table?
=========================================

If you are not familiar with the ``CREATE TABLE`` statement, please refer to the Data Definition Language :doc:`/sql_language/ddl`.

In order to specify a certain file format for your table, you need to use the ``USING`` clause in your ``CREATE TABLE``
statement. Below is an example statement for creating a table using RCFile.

.. code-block:: sql

  CREATE TABLE table1 (
    id int,
    name text,
    score float,
    type text
  ) USING RCFILE;

=========================================
Physical Properties
=========================================

Some table storage formats provide parameters for enabling or disabling features and adjusting physical parameters.
The ``WITH`` clause in the CREATE TABLE statement allows users to set those parameters.

Now, the RCFile storage type provides the following physical properties.

* ``rcfile.serde`` : custom (De)serializer class. ``org.apache.tajo.storage.BinarySerializerDeserializer`` is the default (de)serializer class.
* ``rcfile.null`` : NULL character. It is only used when a table uses ``org.apache.tajo.storage.TextSerializerDeserializer``. The default NULL character is an empty string ``''``. Hive's default NULL character is ``'\\N'``.
* ``compression.codec`` : Compression codec. You can enable compression feature and set specified compression algorithm. The compression algorithm used to compress files. The compression codec name should be the fully qualified class name inherited from `org.apache.hadoop.io.compress.CompressionCodec <https://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/compress/CompressionCodec.html>`_. By default, compression is disabled.

The following is an example for creating a table using RCFile that uses compression.

.. code-block:: sql

  CREATE TABLE table1 (
    id int,
    name text,
    score float,
    type text
  ) USING RCFILE WITH ('compression.codec'='org.apache.hadoop.io.compress.SnappyCodec');

=========================================
RCFile (De)serializers
=========================================

Tajo provides two built-in (De)serializer for RCFile:

* ``org.apache.tajo.storage.TextSerializerDeserializer``: stores column values in a plain-text form.
* ``org.apache.tajo.storage.BinarySerializerDeserializer``: stores column values in a binary file format.

The RCFile format can store some metadata in the RCFile header. Tajo writes the (de)serializer class name into
the metadata header of each RCFile when the RCFile is created in Tajo.

.. note::

  ``org.apache.tajo.storage.BinarySerializerDeserializer`` is the default (de) serializer for RCFile.


=========================================
Compatibility Issues with Apache Hive™
=========================================

Regardless of whether the RCFiles are written by Apache Hive™ or Apache Tajo™, the files are compatible in both systems.
In other words, Tajo can process RCFiles written by Apache Hive and vice versa.

Since there are no metadata in RCFiles written by Hive, we need to manually specify the (de)serializer class name
by setting a physical property.

In Hive, there are two SerDe, and they correspond to the following (de)serializer in Tajo.

* ``org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe``: corresponds to ``TextSerializerDeserializer`` in Tajo.
* ``org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe``: corresponds to ``BinarySerializerDeserializer`` in Tajo.

The compatibility issue mostly occurs when a user creates an external table pointing to data of an existing table.
The following section explains two cases: 1) the case where Tajo reads RCFile written by Hive, and
2) the case where Hive reads RCFile written by Tajo.

-----------------------------------------
When Tajo reads RCFile generated in Hive
-----------------------------------------

To create an external RCFile table generated with ``ColumnarSerDe`` in Hive,
you should set the physical property ``rcfile.serde`` in Tajo as follows:

.. code-block:: sql

  CREATE EXTERNAL TABLE table1 (
    id int,
    name text,
    score float,
    type text
  ) USING RCFILE with ( 'rcfile.serde'='org.apache.tajo.storage.TextSerializerDeserializer', 'rcfile.null'='\\N' )
  LOCATION '....';

To create an external RCFile table generated with ``LazyBinaryColumnarSerDe`` in Hive,
you should set the physical property ``rcfile.serde`` in Tajo as follows:

.. code-block:: sql

  CREATE EXTERNAL TABLE table1 (
    id int,
    name text,
    score float,
    type text
  ) USING RCFILE WITH ('rcfile.serde' = 'org.apache.tajo.storage.BinarySerializerDeserializer')
  LOCATION '....';

.. note::

  As we mentioned above, ``BinarySerializerDeserializer`` is the default (de) serializer for RCFile.
  So, you can omit the ``rcfile.serde`` only for ``org.apache.tajo.storage.BinarySerializerDeserializer``.

-----------------------------------------
When Hive reads RCFile generated in Tajo
-----------------------------------------

To create an external RCFile table written by Tajo with ``TextSerializerDeserializer``,
you should set the ``SERDE`` as follows:

.. code-block:: sql

  CREATE TABLE table1 (
    id int,
    name string,
    score float,
    type string
  ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' STORED AS RCFILE
  LOCATION '<hdfs_location>';

To create an external RCFile table written by Tajo with ``BinarySerializerDeserializer``,
you should set the ``SERDE`` as follows:

.. code-block:: sql

  CREATE TABLE table1 (
    id int,
    name string,
    score float,
    type string
  ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe' STORED AS RCFILE
  LOCATION '<hdfs_location>';