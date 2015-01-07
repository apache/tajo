*************************************
Tajo JDBC Driver
*************************************

Apache Tajo™ provides JDBC driver
which enables Java applciations to easily access Apache Tajo in a RDBMS-like manner.
In this section, we explain how to get JDBC driver and an example code.

How to get JDBC driver
=======================

From Binary Distribution
--------------------------------

Tajo binary distribution provides JDBC jar file and its dependent JAR files.
Those files are located in ``${TAJO_HOME}/share/jdbc-dist/``.


From Building Source Code
--------------------------------

You can build Tajo from the source code and then get JAR files as follows:

.. code-block:: bash

  $ tar xzvf tajo-x.y.z-src.tar.gz
  $ mvn clean package -DskipTests -Pdist -Dtar
  $ ls -l tajo-dist/target/tajo-x.y.z/share/jdbc-dist


Setting the CLASSPATH
=======================

In order to use the JDBC driver, you should set the jar files included in 
``tajo-dist/target/tajo-x.y.z/share/jdbc-dist`` to your ``CLASSPATH``.
In addition, you should add hadoop clsspath into your ``CLASSPATH``.
So, ``CLASSPATH`` will be set as follows:

.. code-block:: bash

  CLASSPATH=path/to/tajo-jdbc/*:path/to/tajo-site.xml:path/to/core-site.xml:path/to/hdfs-site.xml

.. note::

  You must add the locations which include Tajo config files (i.e., ``tajo-site.xml``) and
  Hadoop config files (i.e., ``core-site.xml`` and ``hdfs-site.xml``) to your ``CLASSPATH``.


An Example JDBC Client
=======================

The JDBC driver class name is ``org.apache.tajo.jdbc.TajoDriver``.
You can get the driver ``Class.forName("org.apache.tajo.jdbc.TajoDriver")``.
The connection url should be ``jdbc:tajo://<TajoMaster hostname>:<TajoMaster client rpc port>/<database name>``.
The default TajoMaster client rpc port is ``26002``.
If you want to change the listening port, please refer :doc:`/configuration/cluster_setup`.

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

      try {
        Class.forName("org.apache.tajo.jdbc.TajoDriver");
      } catch (ClassNotFoundException e) {
        // fill your handling code
      }

      Connection conn = DriverManager.getConnection("jdbc:tajo://127.0.0.1:26002/default");

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


FAQ
===========================================

java.nio.channels.UnresolvedAddressException
--------------------------------------------

When retriving the final result, Tajo JDBC Driver tries to access HDFS data nodes.
So, the network access between JDBC client and HDFS data nodes must be available.
In many cases, a HDFS cluster is built in a private network which use private hostnames.
So, the host names must be shared with the JDBC client side.

