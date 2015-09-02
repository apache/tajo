*************************************
PostgreSQL Storage Handler
*************************************

Overview
========

PostgreSQL storage handler available by default in Tajo. It enables users' queries to access database objects in PostgreSQL. Tables in PostgreSQL will be shown as tables in Tajo too. Most of the SQL queries used for PostgreSQL are available in Tajo via this storage handles. Its main advantages is to allow federated query processing among tables in stored HDFS and PostgreSQL.

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
        }
      }
    }
  }

Its configuration change add the tablespace named ``pgsql_db1`` mapped to the database ``db1`` in PostgreSQL. ``uri`` should be just a JDBC connection url. ``mapped_database`` is a database name shown in Tajo for the database ``db1`` in PostgreSQL. The tablespace will be available after you restart a tajo cluster.