*************************************
Tablespaces
*************************************

Tablespaces in Tajo allow users to define locations in the storage system where the files or data objects representing database objects can be stored. Once defined, a tablespace can be referred to by name when creating a database or a table. Especially, it is very useful when a Tajo cluster instance should use heterogeneous storage systems such as HDFS, MySQL, and Oracle.

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

  Also, each tablespace can use different storage type. Please see :doc:`/storage_plugin` if you want to know more information about it.