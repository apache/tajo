******************************
Catalog Configuration
******************************

If you want to customize the catalog service, copy ``$TAJO_HOME/conf/catalog-site.xml.template`` to ``catalog-site.xml``. Then, add the following configs to catalog-site.xml. Note that the default configs are enough to launch Tajo cluster in most cases.

* tajo.catalog.master.addr - If you want to launch a Tajo cluster in distributed mode, you must specify this address. For more detail information, see [Default Ports](#DefaultPorts).
* tajo.catalog.store.class - If you want to change the persistent storage of the catalog server, specify the class name. Its default value is tajo.catalog.store.DerbyStore. In the current version, Tajo provides three persistent storage classes as follows:

+--------------------------------------+------------------------------------------------+
| Driver Class                         | Descriptions                                   |
+======================================+================================================+
| tajo.catalog.store.DerbyStore        | this storage class uses Apache Derby.          |
+--------------------------------------+------------------------------------------------+
| tajo.catalog.store.MySQLStore        | this storage class uses MySQL.                 |
+--------------------------------------+------------------------------------------------+
| tajo.catalog.store.MariaDBStore      | this storage class uses MariaDB.               |
+--------------------------------------+------------------------------------------------+
| tajo.catalog.store.MemStore          | this is the in-memory storage. It is only used |
|                                      | in unit tests to shorten the duration of unit  |
|                                      | tests.                                         |
+--------------------------------------+------------------------------------------------+
| tajo.catalog.store.HiveCatalogStore  | this storage class uses HiveMetaStore.         |
+--------------------------------------+------------------------------------------------+

=========================
Derby Configuration
=========================

By default, Tajo uses `Apache Derby <http://db.apache.org/derby/>`_ as a persistent storage in order to manage table meta data. So, without any configuration, you can use Derby for catalog store.

Also, you can set manually configs in ``conf/catalog-site.xml`` as follows:

.. code-block:: xml

  <property>
    <name>tajo.catalog.store.class</name>
    <value>org.apache.tajo.catalog.store.DerbyStore</value>
  </property>

  <property>
    <name>tajo.catalog.uri</name>
    <value>jdbc:derby:<absolute directory>;create=true</value>
  </property>

Since Derby is a file-based embedded database, it stores data into a specified directory. So, you need to specify the directory for storing data files instead of specifying JDBC URI with hostname and port. For example, in case where you use '/var/data/tajo-catalog' as a derby store directory, you should set configs as follows:

.. code-block:: xml
  
  <property>
    <name>tajo.catalog.uri</name>
    <value>jdbc:derby:/var/data/tajo-catalog;create=true</value>
  </property>

.. warning::

  By default, *Catalog server* stores catalog data into ``/tmp/tajo-catalog-${username}`` directory. But, some operating systems may remove all contents in ``/tmp`` when booting up. In order to ensure persistent store of your catalog data, you need to set a proper location of derby directory.

==================================================
MySQL/MariaDB/PostgreSQL/Oracle Configuration
==================================================

Tajo supports several database systems, including MySQL, MariaDB, PostgreSQL, and Oracle, as its catalog store.
In order to use these systems, you first need to create a database and a user for Tajo.
The following example shows the creation of a user and a database with MySQL.

.. code-block:: sh
  
  mysql> create user 'tajo'@'localhost' identified by 'xxxxxx';
  Query OK, 0 rows affected (0.00 sec)

  mysql> create database tajo;
  Query OK, 1 row affected (0.00 sec)  

  mysql> grant all on tajo.* to 'tajo'@'localhost';
  Query OK, 0 rows affected (0.01 sec)


Second, you must install the proper JDBC driver on the TajoMaster node. And then, you need to set the ``TAJO_CLASSPATH`` variable in ``conf/tajo-env.sh`` as follows:

.. code-block:: sh

  (MySQL)
  $ export TAJO_CLASSPATH=/usr/local/mysql/lib/mysql-connector-java-x.x.x.jar

  (MariaDB)
  $ export TAJO_CLASSPATH=/usr/local/mariadb/lib/mariadb-java-client-x.x.x.jar

  (PostgreSQL)
  $ export TAJO_CLASSPATH=/usr/share/java/postgresql-jdbc4.jar

  (Oracle)
  $ export TAJO_CLASSPATH=/path/to/oracle/driver/ojdbc7.jar

Alternatively, you can copy the jdbc driver into ``$TAJO_HOME/lib``.

Finally, you must add the following configurations to `conf/catalog-site.xml` :

.. code-block:: xml

  <property>
    <name>tajo.catalog.connection.id</name>
    <value><user name></value>
  </property>
  <property>
    <name>tajo.catalog.connection.password</name>
    <value><user password></value>
  </property>

  <!-- MySQL -->
  <property>
    <name>tajo.catalog.store.class</name>
    <value>org.apache.tajo.catalog.store.MySQLStore</value>
  </property>
  <property>
    <name>tajo.catalog.uri</name>
    <value>jdbc:mysql://<mysql host name>:<mysql port>/<database name for tajo>?createDatabaseIfNotExist=true</value>
  </property>

  <!-- MariaDB -->
  <property>
    <name>tajo.catalog.store.class</name>
    <value>org.apache.tajo.catalog.store.MariaDBStore</value>
  </property>
  <property>
    <name>tajo.catalog.uri</name>
    <value>jdbc:mariadb://<mariadb host name>:<mariadb port>/<database name for tajo>?createDatabaseIfNotExist=true</value>
  </property>

  <!-- PostgreSQL -->
  <property>
    <name>tajo.catalog.store.class</name>
    <value>org.apache.tajo.catalog.store.PostgreSQLStore</value>
  </property>
  <property>
    <name>tajo.catalog.uri</name>
    <value>jdbc:postgresql://<postgresql host name>:<postgresql port>/<database name for tajo>?createDatabaseIfNotExist=true</value>
  </property>

  <!-- Oracle -->
  <property>
    <name>tajo.catalog.store.class</name>
    <value>org.apache.tajo.catalog.store.OracleStore</value>
  </property>
  <property>
    <name>tajo.catalog.uri</name>
    <value>jdbc:oracle:thin:@//<oracle host name>:<oracle port>/<ServiceName for tajo database></value>
  </property>

==================================
HiveCatalogStore Configuration
==================================

Tajo support HiveCatalogStore to integrate with hive. If you want to use HiveCatalogStore, you just do as follows.

First, you must compile source code and get a binary archive as follows:

.. code-block:: sh

  $ git clone https://git-wip-us.apache.org/repos/asf/tajo.git tajo
  $ mvn clean install -DskipTests -Pdist -Dtar
  $ ls tajo-dist/target/tajo-x.y.z-SNAPSHOT.tar.gz

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
    <value>org.apache.tajo.catalog.store.HiveCatalogStore</value>
  </property>
