***********
Compression
***********

Using compression can make data size compact, thereby enabling efficient use of network bandwidth and storage. Most of Tajo data formats support data compression feature.
Currently, compression configuration affects only for stored data format and it is enabled when a table is created with the proper table property(See `Create Table <../sql_language/ddl.html#create-table>`_).

===========================================
Compression Properties for each Data Format
===========================================

 .. csv-table:: Compression Properties

  **Data Format**,**Property Name**,**Avaliable Values**
  :doc:`text</table_management/text>`/:doc:`json</table_management/json>`/:doc:`rcfile</table_management/rcfile>`/:doc:`sequencefile</table_management/sequencefile>` [#f1]_,compression.codec,Fully Qualified Classname in Hadoop [#f2]_
  :doc:`parquet</table_management/parquet>`,parquet.compression,uncompressed/snappy/gzip/lzo
  :doc:`orc</table_management/orc>`,orc.compression.kind,none/snappy/zlib

.. rubric:: Footnotes

.. [#f1] For sequence file, you should specify 'compression.type' in addition to 'compression.codec'. Refer to :doc:`/table_management/sequencefile`.
.. [#f2] All classes are available if they implement `org.apache.hadoop.io.compress.CompressionCodec <https://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/compress/CompressionCodec.html>`_.
