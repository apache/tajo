*********************
Worker Configuration
*********************

========================
Worker Heap Memory Size
========================

The environment variable ``TAJO_WORKER_HEAPSIZE`` in ``conf/tajo-env.sh`` allow Tajo Worker to use the specified heap memory size.

If you want to adjust heap memory size, set ``TAJO_WORKER_HEAPSIZE`` variable in ``conf/tajo-env.sh`` with a proper size as follows:

.. code-block:: bash

  TAJO_WORKER_HEAPSIZE=8000

The default size is 1000 (1GB).

========================
Temporary Data Directory
========================

TajoWorker stores temporary data on local file system due to out-of-core algorithms. It is possible to specify one or more temporary data directories where temporary data will be stored.

``tajo-site.xml``

.. code-block:: xml

  <property>
    <name>tajo.worker.tmpdir.locations</name>
    <value>/disk1/tmpdir,/disk2/tmpdir,/disk3/tmpdir</value>
  </property>
  

==========================================================
Maximum number of parallel running tasks for each worker
==========================================================

In Tajo, the capacity of running tasks in parallel are determined by available resources and workload of running queries. In order to specify it, please see [Worker Resources] (#ResourceConfiguration) section.

==========================================================
Worker Resources
==========================================================

Each worker can execute multiple tasks simultaneously.

In Tajo, users can specify the number of cpu cores, the total size of memory and the number of disks for each worker. Available resources affect how many tasks are executed simultaneously.
CPU cores are a unit for expressing CPU parallelism, the unit for memory is megabytes and the unit for disks is the number of disk

In order to specify the resource capacity of each worker, you should add the following configs to ``tajo-site.xml`` :

===================================  =============   ======================   =================================
  property name                        value type      default value            description
===================================  =============   ======================   =================================
  tajo.worker.resource.cpu-cores       Integer         available cpu-cores      the number of cpu cores
  tajo.worker.resource.memory-mb       Integer         available jvm heap       memory size (MB)
  tajo.worker.resource.disks           Integer         1                        the number of disks
  tajo.task.resource.min.memory-mb     Integer         500                      minimum allocatable memory per task
  tajo.qm.resource.min.memory-mb       Integer         500                      minimum allocatable memory per query
===================================  =============   ======================   =================================

.. note:: 
  
  Currently, QueryMaster requests 500MB memory and 1 cpu-core per task for the backward compatibility.
  If you want to give more memory, you can set to ``tajo.qm.resource.min.memory-mb``

.. note::

  If ``tajo.worker.resource.dfs-dir-aware`` is set to ``true`` in ``tajo-site.xml``, the worker will aware of and use the number of HDFS datanode's data dirs in the node.
  In other words, ``tajo.worker.resource.disks`` is ignored.

------------
 Example
------------

Assume that you want to give 15GB Jvm heap, 1GB memory per task, 4 disks, and 12 cores on each worker. The example configuration is as follows:

``tajo-env.sh``

.. code-block:: bash

  export TAJO_WORKER_HEAPSIZE=15000


``tajo-site.xml``

.. code-block:: xml

  <property>
    <name>tajo.worker.resource.cpu-cores</name>
    <value>12</value>
  </property>
  
   <property>
    <name>tajo.task.resource.min.memory-mb</name>
    <value>1000</value>
  </property>
  
  <property>
    <name>tajo.worker.resource.disks</name>
    <value>4</value>
  </property>


* Example with HDFS

``tajo-env.sh``

.. code-block:: bash

  export TAJO_WORKER_HEAPSIZE=15000


``tajo-site.xml``

.. code-block:: xml

   <property>
    <name>tajo.task.resource.min.memory-mb</name>
    <value>1000</value>
  </property>

  <property>
    <name>tajo.worker.resource.dfs-dir-aware</name>
    <value>true</value>
  </property>


* Example with S3

``tajo-env.sh``

.. code-block:: bash

  export TAJO_WORKER_HEAPSIZE=15000


``tajo-site.xml``

.. code-block:: xml

   <property>
    <name>tajo.task.resource.min.memory-mb</name>
    <value>1000</value>
  </property>

  <property>
    <name>tajo.worker.resource.disk.parallel-execution.num</name>
    <value>4</value>
  </property>