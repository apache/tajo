*********************************
Session Variables
*********************************

Once a tajo client connects to the Tajo master, it assigns a unique session. This session is kept until the client is disconnected or it is expired.

For the sake of more convenient user configuration, Tajo provides `session variables`.
With session variables, different configurations are allowed for each session.

``tsql`` provides the meta command ``\set`` to manipulate session variables. Just ``\set`` command shows all session variables. ::

  default> \set
  'name1'='val1'
  'name2'='val2'
  'name3'='val3'
       ...

``\set key val`` will set the session variable named *key* with the value *val*. ::

  default> \set
  'CURRENT_DATABASE'='default'

  default> \set key1 val1

  default> \set
  'CURRENT_DATABASE'='default'
  'key1'='val1'


Also, ``\unset key`` will unset the session variable named *key*.


Currently, tajo provides the following session variables.

.. describe:: BROADCAST_NON_CROSS_JOIN_THRESHOLD

A threshold for non-cross joins. When a non-cross join query is executed with the broadcast join, the whole size of broadcasted tables won't exceed this threshold.

  * Configuration name: :ref:`tajo.dist-query.broadcast.non-cross-join.limit-kb`
  * Property value: Integer
  * Unit: KB
  * Default value: 5120
  * Example

.. code-block:: sh

  \set BROADCAST_NON_CROSS_JOIN_THRESHOLD 5120

.. describe:: BROADCAST_CROSS_JOIN_THRESHOLD

A threshold for cross joins. When a cross join query is executed, the whole size of broadcasted tables won't exceed this threshold.

  * Configuration name: :ref:`tajo.dist-query.broadcast.cross-join.limit-kb`
  * Property value: Integer
  * Unit: KB
  * Default value: 1024
  * Example

.. code-block:: sh

  \set BROADCAST_CROSS_JOIN_THRESHOLD 1024

.. warning::
  In Tajo, the broadcast join is only the way to perform cross joins. Since the cross join is a very expensive operation, this value need to be tuned carefully.

.. describe:: JOIN_TASK_INPUT_SIZE

The repartition join is executed in two stages. When a join query is executed with the repartition join, this value indicates the amount of input data processed by each task at the second stage.
As a result, it determines the degree of the parallel processing of the join query.

  * Configuration name: :ref:`tajo.dist-query.join.task-volume-mb`
  * Property value: Integer
  * Unit: MB
  * Default value: 64
  * Example

.. code-block:: sh

  \set JOIN_TASK_INPUT_SIZE 64

.. describe:: JOIN_PER_SHUFFLE_SIZE

The repartition join is executed in two stages. When a join query is executed with the repartition join,
this value indicates the output size of each task at the first stage, which determines the number of partitions to be shuffled between two stages.

  * Configuration name: :ref:`tajo.dist-query.join.partition-volume-mb`
  * Property value: Integer
  * Unit: MB
  * Default value: 128
  * Example

.. code-block:: sh

  \set JOIN_PER_SHUFFLE_SIZE 128

.. describe:: HASH_JOIN_SIZE_LIMIT

This value provides the criterion to decide the algorithm to perform a join in a task.
If the input data is smaller than this value, join is performed with the in-memory hash join.
Otherwise, the sort-merge join is used.

  * Configuration name: :ref:`tajo.executor.join.common.in-memory-hash-limit-mb`
  * Property value: Integer
  * Unit: MB
  * Default value: 64
  * Example

.. code-block:: sh

  \set HASH_JOIN_SIZE_LIMIT 64

.. warning::
  This value is the size of the input stored on file systems. So, when the input data is loaded into JVM heap,
  its actual size is usually much larger than the configured value, which means that too large threshold can cause unexpected OutOfMemory errors.
  This value should be tuned carefully.

.. describe:: INNER_HASH_JOIN_SIZE_LIMIT

This value provides the criterion to decide the algorithm to perform an inner join in a task.
If the input data is smaller than this value, the inner join is performed with the in-memory hash join.
Otherwise, the sort-merge join is used.

  * Configuration name: :ref:`tajo.executor.join.inner.in-memory-hash-limit-mb`
  * Property value: Integer
  * Unit: MB
  * Default value: 64
  * Example

.. code-block:: sh

  \set INNER_HASH_JOIN_SIZE_LIMIT 64

.. warning::
  This value is the size of the input stored on file systems. So, when the input data is loaded into JVM heap,
  its actual size is usually much larger than the configured value, which means that too large threshold can cause unexpected OutOfMemory errors.
  This value should be tuned carefully.

.. describe:: OUTER_HASH_JOIN_SIZE_LIMIT

This value provides the criterion to decide the algorithm to perform an outer join in a task.
If the input data is smaller than this value, the outer join is performed with the in-memory hash join.
Otherwise, the sort-merge join is used.

  * Configuration name: :ref:`tajo.executor.join.outer.in-memory-hash-limit-mb`
  * Property value: Integer
  * Unit: MB
  * Default value: 64
  * Example

.. code-block:: sh

  \set OUTER_HASH_JOIN_SIZE_LIMIT 64

.. warning::
  This value is the size of the input stored on file systems. So, when the input data is loaded into JVM heap,
  its actual size is usually much larger than the configured value, which means that too large threshold can cause unexpected OutOfMemory errors.
  This value should be tuned carefully.

.. describe:: JOIN_HASH_TABLE_SIZE

The initial size of hash table for in-memory hash join.

  * Configuration name: :ref:`tajo.executor.join.hash-table.size`
  * Property value: Integer
  * Default value: 100000
  * Example

.. code-block:: sh

  \set JOIN_HASH_TABLE_SIZE 100000

.. describe:: SORT_TASK_INPUT_SIZE

The sort operation is executed in two stages. When a sort query is executed, this value indicates the amount of input data processed by each task at the second stage.
As a result, it determines the degree of the parallel processing of the sort query.

  * Configuration name: :ref:`tajo.dist-query.sort.task-volume-mb`
  * Property value: Integer
  * Unit: MB
  * Default value: 64
  * Example

.. code-block:: sh

  \set SORT_TASK_INPUT_SIZE 64

.. describe:: EXTSORT_BUFFER_SIZE

A threshold to choose the sort algorithm. If the input data is larger than this threshold, the external sort algorithm is used.

  * Configuration name: :ref:`tajo.executor.external-sort.buffer-mb`
  * Property value: Integer
  * Unit: MB
  * Default value: 200
  * Example

.. code-block:: sh

  \set EXTSORT_BUFFER_SIZE 200

.. describe:: SORT_LIST_SIZE

The initial size of list for in-memory sort.

  * Configuration name: :ref:`tajo.executor.sort.list.size`
  * Property value: Integer
  * Default value: 100000
  * Example

.. code-block:: sh

  \set SORT_LIST_SIZE 100000

.. describe:: GROUPBY_MULTI_LEVEL_ENABLED

A flag to enable the multi-level algorithm for distinct aggregation. If this value is set, 3-phase aggregation algorithm is used.
Otherwise, 2-phase aggregation algorithm is used.

  * Configuration name: :ref:`tajo.dist-query.groupby.multi-level-aggr`
  * Property value: Boolean
  * Default value: true
  * Example

.. code-block:: sh

  \set GROUPBY_MULTI_LEVEL_ENABLED true

.. describe:: GROUPBY_PER_SHUFFLE_SIZE

The aggregation is executed in two stages. When an aggregation query is executed,
this value indicates the output size of each task at the first stage, which determines the number of partitions to be shuffled between two stages.

  * Configuration name: :ref:`tajo.dist-query.groupby.partition-volume-mb`
  * Property value: Integer
  * Unit: MB
  * Default value: 256
  * Example

.. code-block:: sh

  \set GROUPBY_PER_SHUFFLE_SIZE 256

.. describe:: GROUPBY_TASK_INPUT_SIZE

The aggregation operation is executed in two stages. When an aggregation query is executed, this value indicates the amount of input data processed by each task at the second stage.
As a result, it determines the degree of the parallel processing of the aggregation query.

  * Configuration name: :ref:`tajo.dist-query.groupby.task-volume-mb`
  * Property value: Integer
  * Unit: MB
  * Default value: 64
  * Example

.. code-block:: sh

  \set GROUPBY_TASK_INPUT_SIZE 64

.. describe:: HASH_GROUPBY_SIZE_LIMIT

This value provides the criterion to decide the algorithm to perform an aggregation in a task.
If the input data is smaller than this value, the aggregation is performed with the in-memory hash aggregation.
Otherwise, the sort-based aggregation is used.

  * Configuration name: :ref:`tajo.executor.groupby.in-memory-hash-limit-mb`
  * Property value: Integer
  * Unit: MB
  * Default value: 64
  * Example

.. code-block:: sh

  \set HASH_GROUPBY_SIZE_LIMIT 64

.. warning::
  This value is the size of the input stored on file systems. So, when the input data is loaded into JVM heap,
  its actual size is usually much larger than the configured value, which means that too large threshold can cause unexpected OutOfMemory errors.
  This value should be tuned carefully.

.. describe:: AGG_HASH_TABLE_SIZE

The initial size of hash table for in-memory aggregation.

  * Configuration name: :ref:`tajo.executor.aggregate.hash-table.size`
  * Property value: Integer
  * Default value: 10000
  * Example

.. code-block:: sh

  \set AGG_HASH_TABLE_SIZE 10000

.. describe:: TIMEZONE

Refer to :doc:`/time_zone`.

  * Configuration name: :ref:`tajo.timezone`
  * Property value: Time zone id
  * Default value: Default time zone of JVM
  * Example

.. code-block:: sh

  \set TIMEZONE GMT+9

.. describe:: DATE_ORDER

Date order specification.

  * Configuration name: :ref:`tajo.datetime.date-order`
  * Property value: One of YMD, DMY, MDY.
  * Default value: YMD
  * Example

.. code-block:: sh

  \set DATE_ORDER YMD

.. describe:: PARTITION_NO_RESULT_OVERWRITE_ENABLED

If this value is true, a partitioned table is overwritten even if a subquery leads to no result. Otherwise, the table data will be kept if there is no result.

  * Configuration name: :ref:`tajo.partition.overwrite.even-if-no-result`
  * Property value: Boolean
  * Default value: false
  * Example

.. code-block:: sh

  \set PARTITION_NO_RESULT_OVERWRITE_ENABLED false

.. describe:: TABLE_PARTITION_PER_SHUFFLE_SIZE

In Tajo, storing a partition table is executed in two stages.
This value indicates the output size of a task of the former stage, which determines the number of partitions to be shuffled between two stages.

  * Configuration name: :ref:`tajo.dist-query.table-partition.task-volume-mb`
  * Property value: Integer
  * Unit: MB
  * Default value: 256
  * Example

.. code-block:: sh

  \set TABLE_PARTITION_PER_SHUFFLE_SIZE 256

.. describe:: ARITHABORT

A flag to indicate how to handle the errors caused by invalid arithmetic operations. If true, a running query will be terminated with an overflow or a divide-by-zero.

  * Configuration name: :ref:`tajo.behavior.arithmetic-abort`
  * Property value: Boolean
  * Default value: false
  * Example

.. code-block:: sh

  \set ARITHABORT false

.. describe:: MAX_OUTPUT_FILE_SIZE

Maximum per-output file size. 0 means infinite.

  * Property value: Integer
  * Unit: MB
  * Default value: 0
  * Example

.. code-block:: sh

  \set MAX_OUTPUT_FILE_SIZE 0

.. describe:: SESSION_EXPIRY_TIME

Session expiry time.

  * Property value: Integer
  * Unit: seconds
  * Default value: 3600
  * Example

.. code-block:: sh

  \set SESSION_EXPIRY_TIME 3600

.. describe:: CLI_COLUMNS

Sets the width for the wrapped format.

  * Property value: Integer
  * Default value: 120
  * Example

.. code-block:: sh

  \set CLI_COLUMNS 120

.. describe:: CLI_NULL_CHAR

Sets the string to be printed in place of a null value.

  * Property value: String
  * Default value: ''
  * Example

.. code-block:: sh

  \set CLI_NULL_CHAR ''

.. describe:: CLI_PAGE_ROWS

Sets the number of rows for paging.

  * Property value: Integer
  * Default value: 100
  * Example

.. code-block:: sh

  \set CLI_PAGE_ROWS 100

.. describe:: CLI_PAGING_ENABLED

Enable paging of result display.

  * Property value: Boolean
  * Default value: true
  * Example

.. code-block:: sh

  \set CLI_PAGING_ENABLED true

.. describe:: CLI_DISPLAY_ERROR_TRACE

Enable display of error trace.

  * Property value: Boolean
  * Default value: true
  * Example

.. code-block:: sh

  \set CLI_DISPLAY_ERROR_TRACE true

.. describe:: CLI_FORMATTER_CLASS

Sets the output format class to display results.

  * Property value: Class name
  * Default value: org.apache.tajo.cli.tsql.DefaultTajoCliOutputFormatter
  * Example

.. code-block:: sh

  \set CLI_FORMATTER_CLASS org.apache.tajo.cli.tsql.DefaultTajoCliOutputFormatter

.. describe:: ON_ERROR_STOP

tsql will exit if an error occurs.

  * Property value: Boolean
  * Default value: false
  * Example

.. code-block:: sh

  \set ON_ERROR_STOP false

.. describe:: NULL_CHAR

Null char of text file output. This value is used when the table property `text.null` is not specified.

  * Property value: String
  * Default value: '\\N'
  * Example

.. code-block:: sh

  \set NULL_CHAR '\\N'

.. describe:: DEBUG_ENABLED

A flag to enable debug mode.

  * Property value: Boolean
  * Default value: false
  * Example

.. code-block:: sh

  \set DEBUG_ENABLED false

.. describe:: FETCH_ROWNUM

The number of rows to be fetched from Master each time.

  * Property value: Integer
  * Default value: 200
  * Example

.. code-block:: sh

  \set FETCH_ROWNUM 200



