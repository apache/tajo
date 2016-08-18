*******************
MongoDB Integration
*******************

Overview
========

Apache Tajo™ storage modules contains a storage plugin for MongoDB which allows Apache Tajo to access databases and collections in MongoDB. In order to use MongoDB storage plugin user needs to follow following steps.

* Download Mongo Java Driver 3.2.2 and add to the class path.
* Setup a MongoDB server in a localhost or another. 
* Configure the cluster using storage-site.json

Download MongoDB Java Driver
==========================

You can download latest version of MongoDB java driver from https://mongodb.github.io/mongo-java-driver/ 
Download the .jar file and place it in the class path. You can place the file in TAJO_HOME or you can place the .jar file somewhere else and configure the CLASS_PATH variable from conf/tajo-env.sh (or tajo-env.cmd). 

Setup a Mongo Server
====================
In order to proceed you need to have a running mongo server instance. If you are new to MongoDB just follow the Getting Started Guide(https://docs.mongodb.com/getting-started/shell/). After setting up the server, find the host and port to connect to the particular server. If it is a local installation by default, the port is 27017.


Configure the cluster
=====================

MongoDB storage handler configuration will be like this.
First you have to register the StorageHandler.Then register the space. Update the storage-site.json as following.

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

