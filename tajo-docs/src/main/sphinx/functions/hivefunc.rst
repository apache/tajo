##############
Hive Functions
##############

Tajo provides a feature to use Hive functions directly without re-compilation or additional code.

*************
Configuration
*************

Only thing to do is registering path to a directory for jar files containing your hive functions.
You can do this by set ``tajo.function.hive.jar-dir`` in ``tajo-site.xml`` like the following.

.. code-block:: xml

  <property>
    <name>tajo.function.hive.jar-dir</name>
    <value>/path/to/hive/function/jar</value>
  </property>

.. note::
  The path should be one in local filesystem. HDFS directory is not supported because of JAVA URI compatability problem.

.. warning::

  The path must point to a directory, not a file. And multiple directory entries are not allowed.
  However, it is possible to load multiple jar files.

***************
Using in detail
***************

=============
Function Name
=============

Tajo reads hive functions override ``org.apache.hadoop.hive.ql.exec.UDF`` class. Function name is used as specified in
``@Description`` annotation. If it doesn't exist, Tajo uses full qualified class name as function name. For example,
it can be like this : ``select com_example_hive_udf_myupper('abcd')``, so it is recommended to use Description annotation.

Additionally if some function signature duplicate occurs, it may throw ``AmbiguousFunctionException``.

============================
Parameter type / Return type
============================

Hive uses *Writable* type of Hadoop in functions, but Tajo uses its internal *Datum* type.
So only some Writable types are supported currently by internal converting.
They are listed below.

==================== =========
Writable             Tajo Type
==================== =========
ByteWritable         INT1
ShortWritable        INT2
IntWritable          INT4
LongWritable         INT8
FloatWritable        FLOAT4
DoubleWritable       FLOAT8
Text                 TEXT
BytesWritable        VARBINARY
DateWritable(*)      DATE
TimestampWritable(*) TIMESTAMP
HiveCharWritable(*)  CHAR
==================== =========

.. note::

  (*) They are in org.apache.hadoop.hive.serde2.io package, others are in org.apache.hadoop.io package.

==========
Limitation
==========

1. Currently, Hive UDAF is not supported. Old UDAF interface is deprecated in Hive,
and new GenericUDAF interface cannot be applied because of function design difference between Tajo and Hive.
For same reason, new GenericUDF functions are not supported in Tajo.

2. Because HDFS directory is not supported, Hive UDF jar files should be copied to each worker directory and each path
should be specified in tajo-site.xml.
