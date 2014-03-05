**************************
Tajo Master Configuration
**************************

================================================
  Tajo Rootdir
================================================

Tajo uses HDFS as a primary storage layer. So, one Tajo cluster instance should have one tajo rootdir. A user is allowed to specific your tajo rootdir as follows:

.. code-block:: xml

  <property>
    <name>tajo.rootdir</name>
    <value>hdfs://namenode_hostname:port/path</value>
  </property>

Tajo rootdir must be a url form like ``scheme://hostname:port/path``. The current implementaion only supports ``hdfs://`` and ``file://`` schemes. The default value is ``file:///tmp/tajo-${user.name}/``.

================================================
TajoMaster Heap Memory Size
================================================

The environment variable TAJO_MASTER_HEAPSIZE in conf/tajo-env.sh allow Tajo Master to use the specified heap memory size.

If you want to adjust heap memory size, set ``TAJO_MASTER_HEAPSIZE`` variable in ``conf/tajo-env.sh`` with a proper size as follows:

.. code-block:: sh

  TAJO_MASTER_HEAPSIZE=2000

The default size is 1000 (1GB). 