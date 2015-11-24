**************************
The storage-site.json File
**************************

This file is for configuring :doc:`table spaces</table_management/tablespaces>`.
The syntax of storage-site.json is like this basically:

.. code:: json

  {
    "spaces": {
      <space config 1>, <space config 2>, ...
    },
    "storages": {
      <storage config 1>, <storage config 2>, ...
    }
  }

------
Spaces
------

This is a section for registering table spaces. Some space config example is here:

.. code:: json

  "spaces": {
    "hbase_cluster1": {
      "uri": "hbase://quorum1:port,quorum2:port/",
      "config": { ... }
    },
    ...
  }

* **space name** : Your own table space name which indicates a specific table space. Alpha-numeric characters and underscore(_) are permitted.
* **uri** : An URI address of a table space
* **config** : It is optional. You can specify it as JSON object to pass to each table space handler.

After you specify a table space, you can use it in `create table statement <../sql_language/ddl.html#create-table>`_.

--------
Storages
--------

This is for registering storage format and custom storage handler class.
Tajo already supports HDFS, HBase, PostgreSQL, Amazon S3, Openstack Swift, etc, thus in usual cases using mentioned storages, you don't have to use ``storages`` configuration.
Anyway for your knowledge, here is one storage conf example(it's quoted from Tajo internal hdfs storage configuration which is already being used).

.. code:: json

  "storages": {
    "hdfs": {
      "handler": "org.apache.tajo.storage.FileTablespace",
      "default-format": "text"
    },
    ...
  }
