*************************************
MongoDB Storage Handler
*************************************

Overview
========

MongoDB storage handler is available by default in Tajo.

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