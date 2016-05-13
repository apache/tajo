**************
Hive Functions
**************

Tajo provides a feature to use Hive functions directly without re-compilation or additional code.

=============
Configuration
=============

Only thing to do is registering path to a directory for jar files containing your hive functions.
You can do this by set ``tajo.function.hive.code-dir`` in ``tajo-site.xml`` like the following.

.. code-block:: xml

  <property>
    <name>tajo.function.hive.code-dir</name>
    <value>/path/to/hive/function/jar</value>
  </property>

.. note::
  The path should be local filesystem. HDFS directory is not supported because of JAVA URI compatability.

.. warning::

  It must be a path to a directory, not a file. And multiple directory entries are not allowed.
  However, it is possible that number of jar files is more than one.

===============
Using in detail
===============

