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
In Tajo, users can specify the total size of memory and the number of disks for each worker. Available resources affect how many tasks are executed simultaneously.

In order to specify the resource capacity of each worker, you should add the following configs to ``tajo-site.xml`` :

=================================  ==========================  ===================   =========================
  property name                     description                value type            default value            
=================================  ==========================  ===================   =========================
  tajo.worker.resource.cpu-cores    the number of cpu cores    integer               1                        
  tajo.worker.resource.memory-mb    memory size (MB)           integer               1024                     
  tajo.worker.resource.disks        the number of disks        integer               1                        
=================================  ==========================  ===================   =========================

.. note:: 
  
  Currently, QueryMaster requests 512MB memory and 0.5 disk per task for the backward compatibility.

.. note::

  If ``tajo.worker.resource.dfs-dir-aware`` is set to ``true`` in ``tajo-site.xml``, the worker will aware of and use the number of HDFS datanode's data dirs in the node.
  In other words, ``tajo.worker.resource.disks`` is ignored.

------------
 Example
------------

Assume that you want to give 5120 MB memory, 4 disks, and 24 cores on each worker. The example configuration is as follows:

``tajo-site.xml``

.. code-block:: xml

  <property>
    <name>tajo.worker.resource.tajo.worker.resource.cpu-cores</name>
    <value>24</value>
  </property>
  
   <property>
    <name>tajo.worker.resource.memory-mb</name>
    <value>5120</value>
  </property>
  
  <property>
    <name>tajo.worker.resource.tajo.worker.resource.disks</name>
    <value>4.0</value>
  </property>  

--------------------
 Dedicated Mode
--------------------
Tajo provides a dedicated mode that allows each worker in a Tajo cluster to use whole available system resources including cpu-cores, memory, and disks. For this mode, a user should add the following config to ``tajo-site.xml`` : 

.. code-block:: xml

  <property>
    <name>tajo.worker.resource.dedicated</name>
    <value>true</value>
  </property>

In addition, it can limit the memory capacity used for Tajo worker as follows:

===============================================  ================================================   ===================   =======================
  property name                                  description                                        value type            default value           
===============================================  ================================================   ===================   =======================
  tajo.worker.resource.dedicated-memory-ratio    how much memory to be used in whole memory         float                 0.8                     
===============================================  ================================================   ===================   =======================