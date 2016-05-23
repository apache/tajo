*************************************
Tajo JDBC Driver
*************************************

Apache Tajo™ provides JDBC driver
which enables Java applciations to easily access Apache Tajo in a RDBMS-like manner.
In this section, we explain how to get JDBC driver and an example code.

How to get JDBC driver
=======================

Direct Download
--------------------------------

You can directly download a JDBC driver jar file (``tajo-jdbc-x.y.z.jar``) from `Downloads <http://tajo.apache.org/downloads.html>`_.

From Binary Distribution
--------------------------------

Tajo binary distribution provides JDBC jar file located in ``${TAJO_HOME}/share/jdbc-dist/tajo-jdbc-x.y.z.jar``.


From Building Source Code
--------------------------------

You can build Tajo from the source code and then get JAR files as follows:

.. code-block:: bash

  $ tar xzvf tajo-x.y.z-src.tar.gz
  $ mvn clean package -DskipTests -Pdist -Dtar
  $ ls -l tajo-dist/target/tajo-x.y.z/share/jdbc-dist/tajo-jdbc-x.y.z.jar


Setting the CLASSPATH
=======================

In order to use the JDBC driver, you should add ``tajo-jdbc-x.y.z.jar`` in your ``CLASSPATH``.

.. code-block:: bash

  CLASSPATH=path/to/tajo-jdbc-x.y.z.jar:$CLASSPATH


Connecting to the Tajo cluster instance
=======================================
A Tajo cluster is represented by a URL. Tajo JDBC driver can take the following URL forms:

 * ``jdbc:tajo://host/``
 * ``jdbc:tajo://host/database``
 * ``jdbc:tajo://host:port/``
 * ``jdbc:tajo://host:port/database``

Each part of URL has the following meanings:

 * ``host`` - The hostname of the TajoMaster. You can put hostname or ip address here.
 * ``port`` - The port number that server is listening. Default port number is 26002.
 * ``database`` - The database name. The default database name is ``default``.

 To connect, you need to get ``Connection`` instance from Java JDBC Driver Manager as follows:

.. code-block:: java

  Connection db = DriverManager.getConnection(url);


Connection Parameters
=====================
Connection parameters lets the JDBC Copnnection to enable or disable additional features. You should use ``java.util.Properties`` to pass your connection parameters into ``Connection``. The following example means that the transmission of ResultSet uses compression and its connection timeout is 15 seconds. 

.. code-block:: java

  String url = "jdbc:tajo://localhost/test";
  Properties props = new Properties();
  props.setProperty("useCompression","true");  // use compression for ResultSet
  props.setProperty("connectTimeout","15000"); // 15 seconds
  Connection conn = DriverManager.getConnection(url, props);

The connection parameters that Tajo currently supports are as follows:

 * ``useCompression = bool`` - Enable compressed transfer for ResultSet.
 * ``defaultRowFetchSize = int`` - Determine the number of rows fetched in ResultSet by one fetch with trip to the Server.
 * ``connectTimeout = int (seconds)`` - The timeout value used for socket connect operations. If connecting to the server takes longer than this value, the connection is broken. The timeout is specified in seconds and a value of zero means that it is disabled.
 * ``socketTimeout = int (seconds)`` - The timeout value used for socket read operations. If reading from the server takes longer than this value, the connection is closed. This can be used as both a brute force global query timeout and a method of detecting network problems. The timeout is specified in seconds and a value of zero means that it is disabled.
 * ``retry = int`` - Number of retry operation. Tajo JDBC driver is resilient against some network or connection problems. It determines how many times the connection will retry.
 * ``timezone = string (timezone id e,g, 'Asia/Tokyo')`` - Each connection has its own client time zone setting.


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

