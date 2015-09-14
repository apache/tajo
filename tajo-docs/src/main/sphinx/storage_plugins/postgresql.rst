*************************************
PostgreSQL Storage Handler
*************************************

Overview
========

PostgreSQL storage handler is available by default in Tajo. It enables users' queries to access database objects in PostgreSQL. Tables in PostgreSQL will be shown as tables in Tajo too. Most of the SQL queries used for PostgreSQL are available in Tajo via this storage handles. Its main advantages is to allow federated query processing among tables in stored HDFS and PostgreSQL.

Configuration
=============

PostgreSQL storage handler is a builtin storage handler. So, you can eaisly register PostgreSQL databases to a Tajo cluster if you just add the following line to ``conf/storage-site.json`` file. If you want to know more information about ``storage-site.json``, please refer to :doc:`/table_management/tablespaces`.

.. code-block:: json

  {
    "spaces": {
      "pgsql_db1": {
        "uri": "jdbc:postgresql://hostname:port/db1"
        
        "configs": {
          "mapped_database": "tajo_db1"
          "connection_properties": {
            "user":     "tajo",
            "password": "xxxx"
          }
        }
      }
    }
  }

``configs`` allows users to specific additional configurations.
``mapped_database`` specifies a database name shown in Tajo. In the example, the database ``db1`` in PostgreSQL
will be mapped to the database ``tajo_db1`` in Tajo.
``connection_properties`` allows users to set JDBC connection parameters.
Please refer to https://jdbc.postgresql.org/documentation/head/connect.html in order to know the details of
PostgreSQL connection parameters.

The storage-site.json will be effective after you restart a tajo cluster.