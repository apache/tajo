*********************************
Session Variables
*********************************


Each client connection to TajoMaster creates a unique session, and the client and TajoMaster uses the session until disconnect. A session provides session variables which are used for various configs per session.

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


Now, tajo provides the following session variables.

* ``DIST_QUERY_BROADCAST_JOIN_THRESHOLD``
* ``DIST_QUERY_JOIN_TASK_VOLUME``
* ``DIST_QUERY_SORT_TASK_VOLUME``
* ``DIST_QUERY_GROUPBY_TASK_VOLUME``
* ``DIST_QUERY_JOIN_PARTITION_VOLUME``
* ``DIST_QUERY_GROUPBY_PARTITION_VOLUME``
* ``DIST_QUERY_TABLE_PARTITION_VOLUME``
* ``EXECUTOR_EXTERNAL_SORT_BUFFER_SIZE``
* ``EXECUTOR_HASH_JOIN_SIZE_THRESHOLD``
* ``EXECUTOR_INNER_HASH_JOIN_SIZE_THRESHOLD``
* ``EXECUTOR_OUTER_HASH_JOIN_SIZE_THRESHOLD``
* ``EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD``
* ``MAX_OUTPUT_FILE_SIZE``
* ``CODEGEN``
* ``CLIENT_SESSION_EXPIRY_TIME``
* ``CLI_MAX_COLUMN``
* ``CLI_NULL_CHAR``
* ``CLI_PRINT_PAUSE_NUM_RECORDS``
* ``CLI_PRINT_PAUSE``
* ``CLI_PRINT_ERROR_TRACE``
* ``CLI_OUTPUT_FORMATTER_CLASS``
* ``CLI_ERROR_STOP``
* ``TIMEZONE``
* ``DATE_ORDER``
* ``TEXT_NULL``
* ``DEBUG_ENABLED``
* ``TEST_BROADCAST_JOIN_ENABLED``
* ``TEST_JOIN_OPT_ENABLED``
* ``TEST_FILTER_PUSHDOWN_ENABLED``
* ``TEST_MIN_TASK_NUM``
* ``BEHAVIOR_ARITHMETIC_ABORT``
* ``RESULT_SET_FETCH_ROWNUM``


