*************************************
Parquet
*************************************

Parquet is a columnar storage format for Hadoop. Parquet is designed to make the advantages of compressed,
efficient columnar data representation available to any project in the Hadoop ecosystem,
regardless of the choice of data processing framework, data model, or programming language.
For more details, please refer to `Parquet File Format <http://parquet.io/>`_.

=========================================
How to Create a Parquet Table?
=========================================

If you are not familiar with ``CREATE TABLE`` statement, please refer to Data Definition Language :doc:`/sql_language/ddl`.

In order to specify a certain file format for your table, you need to use the ``USING`` clause in your ``CREATE TABLE``
statement. Below is an example statement for creating a table using parquet files.

.. code-block:: sql

  CREATE TABLE table1 (
    id int,
    name text,
    score float,
    type text
  ) USING PARQUET;

=========================================
Physical Properties
=========================================

Some table storage formats provide parameters for enabling or disabling features and adjusting physical parameters.
The ``WITH`` clause in the CREATE TABLE statement allows users to set those parameters.

Now, Parquet file provides the following physical properties.

* ``parquet.block.size``: The block size is the size of a row group being buffered in memory. This limits the memory usage when writing. Larger values will improve the I/O when reading but consume more memory when writing. Default size is 134217728 bytes (= 128 * 1024 * 1024).
* ``parquet.page.size``: The page size is for compression. When reading, each page can be decompressed independently. A block is composed of pages. The page is the smallest unit that must be read fully to access a single record. If this value is too small, the compression will deteriorate. Default size is 1048576 bytes (= 1 * 1024 * 1024).
* ``parquet.compression``: The compression algorithm used to compress pages. It should be one of ``uncompressed``, ``snappy``, ``gzip``, ``lzo``. Default is ``uncompressed``.
* ``parquet.enable.dictionary``: The boolean value is to enable/disable dictionary encoding. It should be one of either ``true`` or ``false``. Default is ``true``.

=========================================
Compatibility Issues with Apache Hive™
=========================================

At the moment, Tajo only supports flat relational tables.
As a result, Tajo's Parquet storage type does not support nested schemas.
However, we are currently working on adding support for nested schemas and non-scalar types (`TAJO-710 <https://issues.apache.org/jira/browse/TAJO-710>`_).