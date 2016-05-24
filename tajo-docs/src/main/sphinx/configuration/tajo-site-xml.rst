**********************
The tajo-site.xml File
**********************

You can add more configurations in the ``tajo-site.xml`` file. Note that you should replicate this file to the whole hosts in your cluster once you edited.
If you are looking for the configurations for the master and the worker, please refer to :doc:`tajo_master_configuration` and :doc:`worker_configuration`.
Also, catalog configurations are found here :doc:`catalog_configuration`.

=========================
Join Query Settings
=========================

""""""""""""""""""""""""""""""""""""""
`tajo.dist-query.join.auto-broadcast`
""""""""""""""""""""""""""""""""""""""

A flag to enable or disable the use of broadcast join.

  * Property value type: Boolean
  * Default value: true
  * Example

.. code-block:: xml

  <property>
    <name>tajo.dist-query.join.auto-broadcast</name>
    <value>true</value>
  </property>

.. _tajo.dist-query.broadcast.non-cross-join.limit-kb:

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
`tajo.dist-query.broadcast.non-cross-join.limit-kb`
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

A threshold for non-cross joins. When a non-cross join query is executed with the broadcast join, the whole size of broadcasted tables won't exceed this threshold.

  * Property value type: Integer
  * Unit: KB
  * Default value: 5120
  * Example

.. code-block:: xml

  <property>
    <name>tajo.dist-query.broadcast.non-cross-join.limit-kb</name>
    <value>5120</value>
  </property>

.. _tajo.dist-query.broadcast.cross-join.limit-kb:

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
`tajo.dist-query.broadcast.cross-join.limit-kb`
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

A threshold for cross joins. When a cross join query is executed, the whole size of broadcasted tables won't exceed this threshold.

  * Property value type: Integer
  * Unit: KB
  * Default value: 1024
  * Example

.. code-block:: xml

  <property>
    <name>tajo.dist-query.broadcast.cross-join.limit-kb</name>
    <value>1024</value>
  </property>

.. warning::
  In Tajo, the broadcast join is only the way to perform cross joins. Since the cross join is a very expensive operation, this value need to be tuned carefully.

.. _tajo.dist-query.join.task-volume-mb:

""""""""""""""""""""""""""""""""""""""
`tajo.dist-query.join.task-volume-mb`
""""""""""""""""""""""""""""""""""""""

The repartition join is executed in two stages. When a join query is executed with the repartition join, this value indicates the amount of input data processed by each task at the second stage.
As a result, it determines the degree of the parallel processing of the join query.

  * Property value type: Integer
  * Unit: MB
  * Default value: 64
  * Example

.. code-block:: xml

  <property>
    <name>tajo.dist-query.join.task-volume-mb</name>
    <value>64</value>
  </property>

.. _tajo.dist-query.join.partition-volume-mb:

"""""""""""""""""""""""""""""""""""""""""""
`tajo.dist-query.join.partition-volume-mb`
"""""""""""""""""""""""""""""""""""""""""""

The repartition join is executed in two stages. When a join query is executed with the repartition join,
this value indicates the output size of each task at the first stage, which determines the number of partitions to be shuffled between two stages.

  * Property value type: Integer
  * Unit: MB
  * Default value: 128
  * Example

.. code-block:: xml

  <property>
    <name>tajo.dist-query.join.partition-volume-mb</name>
    <value>128</value>
  </property>

.. _tajo.executor.join.common.in-memory-hash-limit-mb:

""""""""""""""""""""""""""""""""""""""""""""""""""""""""
`tajo.executor.join.common.in-memory-hash-limit-mb`
""""""""""""""""""""""""""""""""""""""""""""""""""""""""

This value provides the criterion to decide the algorithm to perform a join in a task.
If the input data is smaller than this value, join is performed with the in-memory hash join.
Otherwise, the sort-merge join is used.

  * Property value type: Integer
  * Unit: MB
  * Default value: 64
  * Example

.. code-block:: xml

  <property>
    <name>tajo.executor.join.common.in-memory-hash-limit-mb</name>
    <value>64</value>
  </property>

.. warning::
  This value is the size of the input stored on file systems. So, when the input data is loaded into JVM heap,
  its actual size is usually much larger than the configured value, which means that too large threshold can cause unexpected OutOfMemory errors.
  This value should be tuned carefully.

.. _tajo.executor.join.inner.in-memory-hash-limit-mb:

""""""""""""""""""""""""""""""""""""""""""""""""""""""""
`tajo.executor.join.inner.in-memory-hash-limit-mb`
""""""""""""""""""""""""""""""""""""""""""""""""""""""""

This value provides the criterion to decide the algorithm to perform an inner join in a task.
If the input data is smaller than this value, the inner join is performed with the in-memory hash join.
Otherwise, the sort-merge join is used.

  * Property value type: Integer
  * Unit: MB
  * Default value: 64
  * Example

.. code-block:: xml

  <property>
    <name>tajo.executor.join.inner.in-memory-hash-limit-mb</name>
    <value>64</value>
  </property>

.. warning::
  This value is the size of the input stored on file systems. So, when the input data is loaded into JVM heap,
  its actual size is usually much larger than the configured value, which means that too large threshold can cause unexpected OutOfMemory errors.
  This value should be tuned carefully.

.. _tajo.executor.join.outer.in-memory-hash-limit-mb:

""""""""""""""""""""""""""""""""""""""""""""""""""""""""
`tajo.executor.join.outer.in-memory-hash-limit-mb`
""""""""""""""""""""""""""""""""""""""""""""""""""""""""

This value provides the criterion to decide the algorithm to perform an outer join in a task.
If the input data is smaller than this value, the outer join is performed with the in-memory hash join.
Otherwise, the sort-merge join is used.

  * Property value type: Integer
  * Unit: MB
  * Default value: 64
  * Example

.. code-block:: xml

  <property>
    <name>tajo.executor.join.outer.in-memory-hash-limit-mb</name>
    <value>64</value>
  </property>

.. warning::
  This value is the size of the input stored on file systems. So, when the input data is loaded into JVM heap,
  its actual size is usually much larger than the configured value, which means that too large threshold can cause unexpected OutOfMemory errors.
  This value should be tuned carefully.

.. _tajo.executor.join.hash-table.size:

"""""""""""""""""""""""""""""""""""""
`tajo.executor.join.hash-table.size`
"""""""""""""""""""""""""""""""""""""

The initial size of hash table for in-memory hash join.

  * Property value type: Integer
  * Default value: 100000
  * Example

.. code-block:: xml

  <property>
    <name>tajo.executor.join.hash-table.size</name>
    <value>100000</value>
  </property>

======================
Sort Query Settings
======================

.. _tajo.dist-query.sort.task-volume-mb:

""""""""""""""""""""""""""""""""""""""
`tajo.dist-query.sort.task-volume-mb`
""""""""""""""""""""""""""""""""""""""

The sort operation is executed in two stages. When a sort query is executed, this value indicates the amount of input data processed by each task at the second stage.
As a result, it determines the degree of the parallel processing of the sort query.

  * Property value type: Integer
  * Unit: MB
  * Default value: 64
  * Example

.. code-block:: xml

  <property>
    <name>tajo.dist-query.sort.task-volume-mb</name>
    <value>64</value>
  </property>

.. _tajo.executor.external-sort.buffer-mb:

""""""""""""""""""""""""""""""""""""""""
`tajo.executor.external-sort.buffer-mb`
""""""""""""""""""""""""""""""""""""""""

A threshold to choose the sort algorithm. If the input data is larger than this threshold, the external sort algorithm is used.

  * Property value type: Integer
  * Unit: MB
  * Default value: 200
  * Example

.. code-block:: xml

  <property>
    <name>tajo.executor.external-sort.buffer-mb</name>
    <value>200</value>
  </property>

.. _tajo.executor.sort.list.size:

""""""""""""""""""""""""""""""""""""""
`tajo.executor.sort.list.size`
""""""""""""""""""""""""""""""""""""""

The initial size of list for in-memory sort.

  * Property value type: Integer
  * Default value: 100000
  * Example

.. code-block:: xml

  <property>
    <name>tajo.executor.sort.list.size</name>
    <value>100000</value>
  </property>

=========================
Group by Query Settings
=========================

.. _tajo.dist-query.groupby.multi-level-aggr:

""""""""""""""""""""""""""""""""""""""""""""
`tajo.dist-query.groupby.multi-level-aggr`
""""""""""""""""""""""""""""""""""""""""""""

A flag to enable the multi-level algorithm for distinct aggregation. If this value is set, 3-phase aggregation algorithm is used.
Otherwise, 2-phase aggregation algorithm is used.

  * Property value type: Boolean
  * Default value: true
  * Example

.. code-block:: xml

  <property>
    <name>tajo.dist-query.groupby.multi-level-aggr</name>
    <value>true</value>
  </property>

.. _tajo.dist-query.groupby.partition-volume-mb:

""""""""""""""""""""""""""""""""""""""""""""""
`tajo.dist-query.groupby.partition-volume-mb`
""""""""""""""""""""""""""""""""""""""""""""""

The aggregation is executed in two stages. When an aggregation query is executed,
this value indicates the output size of each task at the first stage, which determines the number of partitions to be shuffled between two stages.

  * Property value type: Integer
  * Unit: MB
  * Default value: 256
  * Example

.. code-block:: xml

  <property>
    <name>tajo.dist-query.groupby.partition-volume-mb</name>
    <value>256</value>
  </property>

.. _tajo.dist-query.groupby.task-volume-mb:

""""""""""""""""""""""""""""""""""""""""""""""
`tajo.dist-query.groupby.task-volume-mb`
""""""""""""""""""""""""""""""""""""""""""""""

The aggregation operation is executed in two stages. When an aggregation query is executed, this value indicates the amount of input data processed by each task at the second stage.
As a result, it determines the degree of the parallel processing of the aggregation query.

  * Property value type: Integer
  * Unit: MB
  * Default value: 64
  * Example

.. code-block:: xml

  <property>
    <name>tajo.dist-query.groupby.task-volume-mb</name>
    <value>64</value>
  </property>

.. _tajo.executor.groupby.in-memory-hash-limit-mb:

""""""""""""""""""""""""""""""""""""""""""""""""""""""""
`tajo.executor.groupby.in-memory-hash-limit-mb`
""""""""""""""""""""""""""""""""""""""""""""""""""""""""

This value provides the criterion to decide the algorithm to perform an aggregation in a task.
If the input data is smaller than this value, the aggregation is performed with the in-memory hash aggregation.
Otherwise, the sort-based aggregation is used.

  * Property value type: Integer
  * Unit: MB
  * Default value: 64
  * Example

.. code-block:: xml

  <property>
    <name>tajo.executor.groupby.in-memory-hash-limit-mb</name>
    <value>64</value>
  </property>

.. warning::
  This value is the size of the input stored on file systems. So, when the input data is loaded into JVM heap,
  its actual size is usually much larger than the configured value, which means that too large threshold can cause unexpected OutOfMemory errors.
  This value should be tuned carefully.

.. _tajo.executor.aggregate.hash-table.size:

""""""""""""""""""""""""""""""""""""""""""
`tajo.executor.aggregate.hash-table.size`
""""""""""""""""""""""""""""""""""""""""""

The initial size of hash table for in-memory aggregation.

  * Property value type: Integer
  * Default value: 10000
  * Example

.. code-block:: xml

  <property>
    <name>tajo.executor.aggregate.hash-table.size</name>
    <value>10000</value>
  </property>

======================
Date/Time Settings
======================

.. _tajo.timezone:

"""""""""""""""""""
`tajo.timezone`
"""""""""""""""""""

Refer to :doc:`/time_zone`.

  * Property value type: Time zone id
  * Default value: Default time zone of JVM
  * Example

.. code-block:: xml

  <property>
    <name>tajo.timezone</name>
    <value>GMT+9</value>
  </property>

.. _tajo.datetime.date-order:

"""""""""""""""""""""""""""
`tajo.datetime.date-order`
"""""""""""""""""""""""""""

Date order specification.

  * Property value type: One of YMD, DMY, MDY.
  * Default value: YMD
  * Example

.. code-block:: xml

  <property>
    <name>tajo.datetime.date-order</name>
    <value>YMD</value>
  </property>

======================
Table partitions
======================

.. _tajo.partition.overwrite.even-if-no-result:

""""""""""""""""""""""""""""""""""""""""""""""""""""
`tajo.partition.overwrite.even-if-no-result`
""""""""""""""""""""""""""""""""""""""""""""""""""""

If this value is true, a partitioned table is overwritten even if a subquery leads to no result. Otherwise, the table data will be kept if there is no result.

  * Property value type: Boolean
  * Default value: false
  * Example

.. code-block:: xml

  <property>
    <name>tajo.partition.overwrite.even-if-no-result</name>
    <value>false</value>
  </property>

.. _tajo.dist-query.table-partition.task-volume-mb:

""""""""""""""""""""""""""""""""""""""""""""""""""""
`tajo.dist-query.table-partition.task-volume-mb`
""""""""""""""""""""""""""""""""""""""""""""""""""""

In Tajo, storing a partition table is executed in two stages.
This value indicates the output size of a task of the former stage, which determines the number of partitions to be shuffled between two stages.

  * Property value type: Integer
  * Unit: MB
  * Default value: 256
  * Example

.. code-block:: xml

  <property>
    <name>tajo.dist-query.table-partition.task-volume-mb</name>
    <value>256</value>
  </property>

======================
Arithmetic Settings
======================

.. _tajo.behavior.arithmetic-abort:

""""""""""""""""""""""""""""""""""""""""""""""""""""
`tajo.behavior.arithmetic-abort`
""""""""""""""""""""""""""""""""""""""""""""""""""""

A flag to indicate how to handle the errors caused by invalid arithmetic operations. If true, a running query will be terminated with an overflow or a divide-by-zero.

  * Property value type: Boolean
  * Default value: false
  * Example

.. code-block:: xml

  <property>
    <name>tajo.behavior.arithmetic-abort</name>
    <value>false</value>
  </property>