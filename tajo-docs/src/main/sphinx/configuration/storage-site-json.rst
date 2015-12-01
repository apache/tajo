**************************
The storage-site.json File
**************************

This file is for configuring :doc:`/table_management/tablespaces`.
The syntax of ``storage-site.json`` is like this basically:

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

This is a section for registering tablespaces. Some space config example is here:

.. code:: json

  "spaces": {
    "jdbc_pgsql": {
      "uri": "jdbc:postgresql://127.0.0.1:5740/origin",
      "configs": {
        "mapped_database":"tajo_pgsql_origin",
        "connection_properties": {
          "user":"tajouser",
          "password":"123456"
        }
      }
    },
    ...
  }

* **space name** : Your own tablespace name which indicates a specific tablespace. Alpha-numeric characters and underscore(_) are permitted.
* **uri** : An URI address of a tablespace
* **configs** : It is optional. You can specify it as JSON object to pass to each tablespace handler.

After you specify a tablespace, you can use it in `create table statement <../sql_language/ddl.html#create-table>`_.

--------
Storages
--------

This is for registering storage format and custom storage handler class.
Tajo already supports HDFS, HBase, PostgreSQL, Amazon S3, Openstack Swift, etc, thus in usual cases using mentioned storages, you don't have to add any ``storages`` configuration.
However, if you want to use your custom storage as one of Tajo's data source, you need to add a configuration for your storage. Here is an example of HDFS storage.
See :doc:`storage_plugins/overview` for more information.

.. code:: json

  "storages": {
    "hdfs": {
      "handler": "org.apache.tajo.storage.FileTablespace",
      "default-format": "text"
    },
    ...
  }
