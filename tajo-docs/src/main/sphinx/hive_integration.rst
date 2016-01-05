****************
Hive Integration
****************

Apache Tajo™ catalog supports HiveCatalogStore to integrate with Apache Hive™.
This integration allows Tajo to access all tables used in Apache Hive. 
Depending on your purpose, you can execute either SQL queries or HiveQL queries on the 
same tables managed in Apache Hive.

In order to use this feature, you need to build Tajo with a specified maven profile 
and then add some configs into ``conf/tajo-env.sh`` and ``conf/catalog-site.xml``. 
This section describes how to setup HiveMetaStore integration.
This instruction would take no more than five minutes.

You need to set your Hive home directory to the environment variable **HIVE_HOME** in ``conf/tajo-env.sh`` as follows:

.. code-block:: sh

  export HIVE_HOME=/path/to/your/hive/directory

If you need to use jdbc to connect HiveMetaStore, you have to prepare MySQL jdbc driver.
Next, you should set the path of MySQL JDBC driver jar file to the environment variable **HIVE_JDBC_DRIVER_DIR** in ``conf/tajo-env.sh`` as follows:

.. code-block:: sh

  export HIVE_JDBC_DRIVER_DIR=/path/to/your/mysql_jdbc_driver/mysql-connector-java-x.x.x-bin.jar

Finally, you should specify HiveCatalogStore as Tajo catalog driver class in ``conf/catalog-site.xml`` as follows:

.. code-block:: xml

  <property>
    <name>tajo.catalog.store.class</name>
    <value>org.apache.tajo.catalog.store.HiveCatalogStore</value>
  </property>

.. note::

  Hive stores a list of partitions for each table in its metastore. When new partitions are
  added directly to HDFS, HiveMetastore can't recognize these partitions until the user executes
  ``ALTER TABLE table_name ADD PARTITION`` commands on each of the newly added partitions or
  ``MSCK REPAIR TABLE table_name`` command.

  But current Tajo doesn't provide ``ADD PARTITION`` command and Hive doesn't provide an api for
  responding to ``MSK REPAIR TABLE`` command. Thus, if you insert data to Hive partitioned
  table and you want to scan the updated partitions through Tajo, you must run following command on Hive
  (see `Hive doc <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-RecoverPartitions(MSCKREPAIRTABLE)>`_
  for more details of the command):

  .. code-block:: sql

    MSCK REPAIR TABLE [table_name];
