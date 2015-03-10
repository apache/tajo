*************************************
Tajo JDBC Driver
*************************************

Apache Tajo™ provides JDBC driver
which enables Java applciations to easily access Apache Tajo in a RDBMS-like manner.
In this section, we explain how to get JDBC driver and an example code.

How to get JDBC driver
=======================

From Tajo Homepage
--------------------------------

You can download JDBC jar file ``tajo-jdbc-x.y.z.jar`` in `Downloads <downloads.html>`_ section.

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

