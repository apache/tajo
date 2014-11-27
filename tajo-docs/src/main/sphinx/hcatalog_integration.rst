*************************************
HCatalog Integration
*************************************

Apache Tajo™ catalog supports HCatalogStore driver to integrate with Apache Hive™. 
This integration allows Tajo to access all tables used in Apache Hive. 
Depending on your purpose, you can execute either SQL queries or HiveQL queries on the 
same tables managed in Apache Hive.

In order to use this feature, you need to build Tajo with a specified maven profile 
and then add some configs into ``conf/tajo-env.sh`` and ``conf/catalog-site.xml``. 
This section describes how to setup HCatalog integration. 
This instruction would take no more than ten minutes.

First, you need to compile the source code with hcatalog profile. 
Currently, Tajo supports hcatalog-0.11.0 and hcatalog-0.12.0 profile.
So, if you want to use Hive 0.11.0, you need to set ``-Phcatalog-0.11.0`` as the maven profile ::

  $ mvn clean package -DskipTests -Pdist -Dtar -Phcatalog-0.11.0

Or, if you want to use Hive 0.12.0, you need to set ``-Phcatalog-0.12.0`` as the maven profile ::

  $ mvn clean package -DskipTests -Pdist -Dtar -Phcatalog-0.12.0

Then, you need to set your Hive home directory to the environment variable ``HIVE_HOME`` in conf/tajo-env.sh as follows: ::

  export HIVE_HOME=/path/to/your/hive/directory

If you need to use jdbc to connect HiveMetaStore, you have to prepare MySQL jdbc driver.
Next, you should set the path of MySQL JDBC driver jar file to the environment variable HIVE_JDBC_DRIVER_DIR in conf/tajo-env.sh as follows: ::

  export HIVE_JDBC_DRIVER_DIR==/path/to/your/mysql_jdbc_driver/mysql-connector-java-x.x.x-bin.jar

Finally, you should specify HCatalogStore as Tajo catalog driver class in ``conf/catalog-site.xml`` as follows: ::

  <property>
    <name>tajo.catalog.store.class</name>
    <value>org.apache.tajo.catalog.store.HCatalogStore</value>
  </property>

.. note::

  Hive stores a list of partitions for each table in its metastore. If new partitions are
  directly added to HDFS, HiveMetastore will not able aware of these partitions unless the user
  ``ALTER TABLE table_name ADD PARTITION`` commands on each of the newly added partitions or
  ``MSCK REPAIR TABLE  table_name`` command.

  But current tajo doesn't provide ``ADD PARTITION`` command and hive doesn't provide an api for
  responding to ``MSK REPAIR TABLE`` command. Thus, if you insert data to hive partitioned
  table and you want to scan the updated partitions through Tajo, you must run following command on hive ::

  $ MSCK REPAIR TABLE [table_name];
