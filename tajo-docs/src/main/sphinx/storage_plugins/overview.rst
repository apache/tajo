*************************************
Storage Plugin Overview
*************************************

Overview
========

Tajo supports various storage systems, such as HDFS, Amazon S3, Openstack Swift, HBase, and RDBMS. Tajo already embeds HDFS, S3, Openstack, HBase, RDBMS storage plugins, and also Tajo allows users to register custom storages and data formats to Tajo cluster instances. This section describes how you register custom storages and data types.

Register custom storage
=======================

First of all, your storage implementation should be packed as a jar file. Then, please copy the jar file into ``tajo/extlib`` directory. Next, you should copy ``conf/storage-site.json.template`` into ``conf/storage-site.json`` and modify the file like the below.

Configuration
=============

Tajo has a default configuration for builtin storages, such as HDFS, local file system, and Amazon S3. it also allows users to add custom storage plugins

``conf/storage-site.json`` file has the following struct:

.. code-block:: json

  {
    "storages": {
      "${scheme}": {
        "handler": "${class name}"
      }
    }
  }

Each storage instance (i.e., :doc:`/table_management/tablespaces`) is identified by an URI. The scheme of URI plays a role to identify storage type. For example, ``hdfs://`` is used for Hdfs storage, ``jdbc://`` is used for JDBC-based storage, and ``hbase://`` is used for HBase storage. 

You should substitute a scheme name without ``://`` for ``${scheme}``.

See an example for HBase storage.

.. code-block:: json

  {
    "storages": {
      "hbase": {
        "handler": "org.apache.tajo.storage.hbase.HBaseTablespace",
        "default-format": "hbase"
      }
    }
  }