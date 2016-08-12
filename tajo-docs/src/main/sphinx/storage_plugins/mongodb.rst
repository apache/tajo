*******************
MongoDB Integration
*******************

Overview
========

Apache Tajo™ storage modules contains a storage plugin for MongoDB which allows Apache Tajo to access databases and collections in MongoDB. In order to use MongoDB storage plugin user needs to follow following steps.

Configuration
=============

MongoDB storage handler configuration will be like this.. First you have to register the StorageHandler. Then register the space. Update the storage-site.json as following.

.. code-block:: json

    {
      "storages": {
        "mongodb": {
            "handler": "org.apache.tajo.storage.mongodb.MongoDBTableSpace",
        "default-format": "text"
        }
      },
        "spaces": {
            "space_name": {
            "uri": "mongodb://<host>:<port>/<database_name>?user=<username>&password=<password>",
            "configs": {
                "mapped_database": "<mapped_database_name>"
                  }
            }
        }
    }

