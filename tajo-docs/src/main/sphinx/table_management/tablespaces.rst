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
Other one is managed table, which means internal table, that is created in a speficied tablespace.

Tablespace is a pre-defined physical location where data stored on. It is supported for only managed tables.
When you create a managed table, you can use the **tablespace** keyword to specify the location of data will be stored.
If the tablespace is not specified, the default tablespace of the table's database is used.

.. note::

  For creating a table, see :doc:`/sql_language/ddl`.

=============
Configuration
=============

By default, Tajo use ``${tajo.rootdir}/warehouse`` in :doc:`conf/tajo-site.xml</configuration/tajo-site-xml>` as a default tablespace. It also allows users to register additional tablespaces using ``storage-site.json`` file like below.

---------------------------
conf/storage-site.json file
---------------------------

The configuration file has the following struct:

.. code-block:: json

  {
    "spaces": {
      "${tablespace_name}": {
        "uri": "hbase:zk://quorum1:port,quorum2:port/"
      }
    }
  }

The following is an example for two tablespaces for hbase and hdfs:

.. code-block:: json

  {
    "spaces": {
      "hbase_cluster1": {
        "uri": "hbase:zk://quorum1:port,quorum2:port/"
      },

      "ssd": {
        "uri": "hdfs://host:port/data/ssd"
      }
    }
  }

For more details, see :doc:`conf/storage-site.json</configuration/storage-site-json>`.


.. note::

  Also, each tablespace can use different storage type. Please see :doc:`/storage_plugins` if you want to know more information about it.