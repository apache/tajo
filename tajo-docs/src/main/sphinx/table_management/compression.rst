***********
Compression
***********

Using compression makes data size compact and network traffic low. Most of Tajo data types support data compression feature.
Currently, compression configuration affcts only for stored data format and it is specified when a table is created as table meta information.
Compression for intermidate data or others is not supported now.

===========================================
Compression Properties for each Data Format
===========================================

 .. csv-table:: Compression Properties and Codec Class

  **Data Format**,**Property Name**,**Avaliable Values**
  text/json/rcfile/sequencefile [#f1]_,compression.codec,Fully Qualified Classname in Hadoop [#f2]_
  parquet,parquet.compression,uncompressed/snappy/gzip/lzo
  orc,orc.compression.kind,none/snappy/zlib

.. rubric:: Footnotes

.. [#f1] For sequence file, you should specify 'compression.type' in addition to 'compression.codec'. Refer to :doc:`/table_management/sequencefile`.
.. [#f2] All classes are available if they implement `org.apache.hadoop.io.compress.CompressionCodec <https://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/compress/CompressionCodec.html>`_.
