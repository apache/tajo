*************************************
MySQL Storage Handler
*************************************

Overview
========

MySQL storage handler is available by default in Tajo. It enables users' queries to access database objects in MySQL. Tables in MySQL will be shown as tables in Tajo too. Most of the SQL queries used for MySQL are available in Tajo via this storage handles. Its main advantages is to allow federated query processing among tables in stored HDFS and MySQL.

Configuration
=============

MySQL storage handler is a builtin storage handler. So, you can eaisly register MySQL databases to a Tajo cluster if you just add the following line to ``conf/storage-site.json`` file. If you want to know more information about ``storage-site.json``, please refer to :doc:`/table_management/tablespaces`.

.. code-block:: json

  {
    "spaces": {
      "mysql_db1": {
        "uri": "jdbc:mysql://hostname:port/db1",
        "configs": {
          "mapped_database": "tajo_db1",
          "connection_properties": {
            "user":     "tajo",
            "password": "xxxx"
          }
        }
      }
    }
  }

``configs`` allows users to specific additional configurations.
``mapped_database`` specifies a database name shown in Tajo. In the example, the database ``db1`` in MySQL will be mapped to the database ``tajo_db1`` in Tajo.
``connection_properties`` allows users to set JDBC connection parameters.
Please refer to https://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html in order to know the details of MySQL connection parameters.

The storage-site.json will be effective after you restart a tajo cluster.

.. warning::

  By default, Tajo uses Standardized database driver which is distributed officially by MySQL for providing MySQLTablespace. You need to copy the jdbc driver into ``$TAJO_HOME/lib`` on all nodes.
  Please refer to https://dev.mysql.com/downloads/connector/j/ in order to get the jdbc driver.