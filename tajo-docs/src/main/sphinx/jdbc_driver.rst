*************************************
Tajo JDBC Driver
*************************************

Apache Tajo™ provides JDBC driver
which enables Java applciations to easily access Apache Tajo in a RDBMS-like manner.
In this section, we explain how to get JDBC driver and an example code.

How to get JDBC driver
=======================

Tajo provides some necesssary jar files packaged by maven. In order get the jar files, 
please follow the below commands.

.. code-block:: bash

  $ cd tajo-x.y.z-incubating
  $ mvn clean package -DskipTests -Pdist -Dtar
  $ ls -l tajo-dist/target/tajo-x.y.z-incubating/share/jdbc-dist


Setting the CLASSPATH
=======================

In order to use the JDBC driver, you should set the jar files included in 
``tajo-dist/target/tajo-x.y.z-incubating/share/jdbc-dist`` to your ``CLASSPATH``.
In addition, you should add hadoop clsspath into your ``CLASSPATH``.
So, ``CLASSPATH`` will be set as follows:

.. code-block:: bash

  CLASSPATH=path/to/tajo-jdbc/*:${TAJO_HOME}/conf:$(hadoop classpath)

.. note::

  You can get ${hadoop classpath} by executing  the command ``bin/hadoop classpath`` in your hadoop cluster.

.. note::

  You may want to a minimal set of JAR files. If so, please refer :ref:`minimal_jar_files`.

An Example JDBC Client
=======================

The JDBC driver class name is ``org.apache.tajo.jdbc.TajoDriver``.
You can get the driver ``Class.forName("org.apache.tajo.jdbc.TajoDriver").newInstance()``.
The connection url should be ``jdbc:tajo://<TajoMaster hostname>:<TajoMaster client rpc port>``.
The default TajoMaster client rpc port is ``26002``.
If you want to change the listening port, please refer :doc:`/configuration/configuration_defaults`.

.. note::
  
  Currently, Tajo does not support the concept of database and namespace. 
  All tables are contained in ``default`` database. So, you don't need to specify any database name.

The following shows an example of JDBC Client.

.. code-block:: java

  import java.sql.Connection;
  import java.sql.ResultSet;
  import java.sql.Statement;
  import java.sql.DriverManager;

  public class TajoJDBCClient {
    
    ....

    public static void main(String[] args) throws Exception {
      Class.forName("org.apache.tajo.jdbc.TajoDriver").newInstance();
      Connection conn = DriverManager.getConnection("jdbc:tajo://127.0.0.1:26002");

      Statement stmt = null;
      ResultSet rs = null;
      try {
        stmt = conn.createStatement();
        rs = stmt.executeQuery("select * from table1");
        while (rs.next()) {
          System.out.println(rs.getString(1) + "," + rs.getString(3));
        }
      } finally {
        if (rs != null) rs.close();
        if (stmt != null) stmt.close();
        if (conn != null) conn.close();
      }
    }
  }


Appendix
===========================================

.. _minimal_jar_files:

Minimal JAR file list
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following JAR files are necessary minimal JAR file list.
We've tested JDBC drivers with the following JAR files for
usual SQL queries. But, they does not guarantee that they are 
fully tested for all operations. So, you may need additional JAR files.
In addition to the following JAR files, please don't forgot including
``${HADOOP_HOME}/eta/hadoop`` and ``${TAJO_HOME}/conf`` in your ``CLASSPATH``.

  * hadoop-annotations-2.2.0.jar
  * hadoop-auth-2.2.0.jar
  * hadoop-common-2.2.0.jar
  * hadoop-hdfs-2.2.0.jar
  * joda-time-2.3.jar
  * tajo-catalog-common-0.8.0-SNAPSHOT.jar
  * tajo-client-0.8.0-SNAPSHOT.jar
  * tajo-common-0.8.0-SNAPSHOT.jar
  * tajo-jdbc-0.8.0-SNAPSHOT.jar
  * tajo-rpc-0.8.0-SNAPSHOT.jar
  * tajo-storage-0.8.0-SNAPSHOT.jar
  * log4j-1.2.17.jar
  * commons-logging-1.1.1.jar
  * guava-11.0.2.jar
  * protobuf-java-2.5.0.jar
  * netty-3.6.2.Final.jar
  * commons-lang-2.5.jar
  * commons-configuration-1.6.jar
  * slf4j-api-1.7.5.jar
  * slf4j-log4j12-1.7.5.jar
  * commons-cli-1.2.jar
  * commons-io-2.1.jar"


FAQ
===========================================

java.nio.channels.UnresolvedAddressException
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When retriving the final result, Tajo JDBC Driver tries to access HDFS data nodes.
So, the network access between JDBC client and HDFS data nodes must be available.
In many cases, a HDFS cluster is built in a private network which use private hostnames.
So, the host names must be shared with the JDBC client side.

