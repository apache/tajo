***********
Tablespaces
***********

Tablespaces in Tajo allow users to define locations in the storage system where the files or data objects representing database objects can be stored.
Once defined, a tablespace can be referred to by name when creating a database or a table.
Especially, it is very useful when a Tajo cluster instance should use heterogeneous storage systems such as HDFS, MySQL, and Oracle.

============================================
External Table, Managed Table and Tablespace
============================================

Tajo has two types of table. One is external table. It needs **location** property when the table is created. Using this property, you can create an external table indicating existing external data source.
For example, if there is already your data as Text/JSON files or HBase table, you can register it as tajo external table.
Other one is managed table, which means internal table, that is created in Tajo internal table space. You can convert external data to the form that you want and save it in managed table.
Tablespace is used for managed tables and it is a kind of alias represents physical(distributed, usually) storages. When managed table is created, you can specify a tablespace with **tablespace** keyword, or default tablespace will be used.

.. note::

  For creating a table, see :doc:`/sql_language/ddl`.

=============
Configuration
=============

By default, Tajo use in ``${tajo.rootdir}/warehouse`` in ``conf/tajo-site.xml`` as a default tablespace. It also allows users to register additional tablespaces. 

``conf/storage-site.json`` file.

The configuration file has the following struct:

.. code-block:: json

  {
    "spaces": {
      "${table_space_name}": {
        "uri": "hbase://quorum1:port,quorum2:port/"
      }
    }
  }

The following is an example for two tablespaces for hbase and hdfs:

.. code-block:: json

  {
    "spaces": {
      "hbase-cluster1": {
        "uri": "hbase://quorum1:port,quorum2:port/"
      },

      "ssd": {
        "uri": "hdfs://host:port/data/ssd"
      }
    }
  }


.. note::

  Also, each tablespace can use different storage type. Please see :doc:`/storage_plugins` if you want to know more information about it.