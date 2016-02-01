***
ORC
***

**ORC(Optimized Row Columnar)** is a columnar storage format from Hive. ORC improves performance for reading,
writing, and processing data.
For more details, please refer to `ORC Files <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+ORC>`_ at Hive wiki.

===========================
How to Create an ORC Table?
===========================

If you are not familiar with ``CREATE TABLE`` statement, please refer to Data Definition Language :doc:`/sql_language/ddl`.

In order to specify a certain file format for your table, you need to use the ``USING`` clause in your ``CREATE TABLE``
statement. Below is an example statement for creating a table using orc files.

.. code-block:: sql

  CREATE TABLE table1 (
    id int,
    name text,
    score float,
    type text
  ) USING orc;

===================
Physical Properties
===================

Some table storage formats provide parameters for enabling or disabling features and adjusting physical parameters.
The ``WITH`` clause in the CREATE TABLE statement allows users to set those parameters.

Now, ORC file provides the following physical properties.

* ``orc.max.merge.distance``: When ORC file is read, if stripes are too closer and the distance is lower than this value, they are merged and read at once. Default is 1MB.
* ``orc.max.read.buffer``: When ORC file is read, it defines maximum read buffer size. That is, it can be maximum size of a single read. Default is 8MB.
* ``orc.stripe.size``: It decides size of each stripe. Default is 64MB.
* ``orc.compression.kind``: It means the compression algorithm used to compress and write data. It should be one of ``none``, ``snappy``, ``zlib``. Default is ``none``.
* ``orc.buffer.size``: It decides size of writing buffer. Default is 256KB.
* ``orc.rowindex.stride``: Define the default ORC index stride in number of rows. (Stride is the number of rows an index entry represents.) Default is 10000.

======================================
Compatibility Issues with Apache Hive™
======================================

At the moment, Tajo only supports flat relational tables.
We are currently working on adding support for nested schemas and non-scalar types (`TAJO-710 <https://issues.apache.org/jira/browse/TAJO-710>`_).