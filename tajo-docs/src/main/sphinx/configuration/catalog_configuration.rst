******************************
Catalog Configuration
******************************

If you want to customize the catalog service, copy ``$TAJO_HOME/conf/catalog-site.xml.template`` to ``catalog-site.xml``. Then, add the following configs to catalog-site.xml. Note that the default configs are enough to launch Tajo cluster in most cases.

* tajo.catalog.master.addr - If you want to launch a Tajo cluster in distributed mode, you must specify this address. For more detail information, see [Default Ports](#DefaultPorts).
* tajo.catalog.store.class - If you want to change the persistent storage of the catalog server, specify the class name. Its default value is tajo.catalog.store.DerbyStore. In the current version, Tajo provides three persistent storage classes as follows:

+-----------------------------------+------------------------------------------------+
| Driver Class                      | Descriptions                                   |
+===================================+================================================+
| tajo.catalog.store.DerbyStore     | this storage class uses Apache Derby.          |
+-----------------------------------+------------------------------------------------+
| tajo.catalog.store.MySQLStore     | this storage class uses MySQL.                 |
+-----------------------------------+------------------------------------------------+
| tajo.catalog.store.MariaDBStore   | this storage class uses MariaDB.               |
+-----------------------------------+------------------------------------------------+
| tajo.catalog.store.MemStore       | this is the in-memory storage. It is only used |
|                                   | in unit tests to shorten the duration of unit  |
|                                   | tests.                                         |
+-----------------------------------+------------------------------------------------+
| tajo.catalog.store.HCatalogStore  | this storage class uses HiveMetaStore.         |
+-----------------------------------+------------------------------------------------+

=========================
MySQLStore Configuration
=========================

In order to use MySQLStore, you need to create database and user on MySQL for Tajo.

.. code-block:: sh
  
  mysql> create user 'tajo'@'localhost' identified by 'xxxxxx';
  Query OK, 0 rows affected (0.00 sec)

  mysql> create database tajo;
  Query OK, 1 row affected (0.00 sec)  

  mysql> grant all on tajo.* to 'tajo'@'localhost';
  Query OK, 0 rows affected (0.01 sec)


And then, you need to prepare MySQL JDBC driver on the machine which can be ran TajoMaster. If you do, you should set ``TAJO_CLASSPATH`` variable in ``conf/tajo-env.sh`` with it as follows:

.. code-block:: sh

  export TAJO_CLASSPATH=/usr/local/mysql/lib/mysql-connector-java-x.x.x.jar

Or you just can copy jdbc driver into ``$TAJO_HOME/lib``.

Finally, you should add the following config to `conf/catalog-site.xml` :

.. code-block:: xml

  <property>
    <name>tajo.catalog.store.class</name>
    <value>org.apache.tajo.catalog.store.MySQLStore</value>
  </property>
  <property>
    <name>tajo.catalog.jdbc.connection.id</name>
    <value><mysql user name></value>
  </property>
  <property>
    <name>tajo.catalog.jdbc.connection.password</name>
    <value><mysql user password></value>
  </property>
  <property>
    <name>tajo.catalog.jdbc.uri</name>
    <value>jdbc:mysql://<mysql host name>:<mysql port>/<database name for tajo>?createDatabaseIfNotExist=true</value>
  </property>


===========================
MariaDBStore Configuration
===========================

All configurations for using MariaDBStore is compatible with MySQLStore except following:

.. code-block:: sh

  export TAJO_CLASSPATH=/usr/local/mariadb/lib/mariadb-java-client-x.x.x.jar

.. code-block:: xml

  <property>
    <name>tajo.catalog.store.class</name>
    <value>org.apache.tajo.catalog.store.MariaDBStore</value>
  </property>
  <property>
    <name>tajo.catalog.jdbc.uri</name>
    <value>jdbc:mariadb://<mariadb host name>:<mariadb port>/<database name for tajo>?createDatabaseIfNotExist=true</value>
  </property>


----------------------------------
  HCatalogStore Configuration
----------------------------------

Tajo support HCatalogStore to integrate with hive. If you want to use HCatalogStore, you just do as follows.

First, you must compile source code and get a binary archive as follows:

.. code-block:: sh

  $ git clone https://git-wip-us.apache.org/repos/asf/tajo.git tajo
  $ mvn clean install -DskipTests -Pdist -Dtar -Phcatalog-0.1x.0
  $ ls tajo-dist/target/tajo-x.y.z-SNAPSHOT.tar.gz

Currently Tajo supports only hive 0.12.0. If you enables HCatalogStore, you set the maven profile as ``-Phcatalog-0.12.0``.

Second, you must set your hive home directory to HIVE_HOME variable in ``conf/tajo-env.sh`` with it as follows:

.. code-block:: sh

  export HIVE_HOME=/path/to/your/hive/directory

Third, if you need to use jdbc to connect HiveMetaStore, you have to prepare mysql jdbc driver on host which can be ran TajoMaster. If you prepare it, you should set jdbc driver file path to ``HIVE_JDBC_DRIVER_DIR`` variable in conf/tajo-env.sh with it as follows:

.. code-block:: sh

  export HIVE_JDBC_DRIVER_DIR=/path/to/your/mysql_jdbc_driver/mysql-connector-java-x.x.x-bin.jar


Lastly, you should add the following config to ``conf/catalog-site.xml`` :

.. code-block:: xml

  <property>
    <name>tajo.catalog.store.class</name>
    <value>org.apache.tajo.catalog.store.HCatalogStore</value>
  </property>
