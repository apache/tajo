**********************************
Setting up a local Tajo cluster
**********************************

Apache Tajo™ provides two run modes: local mode and fully distributed mode. Here, we explain only the local mode where a Tajo instance runs on a local file system. A local mode Tajo instance can start up with very simple configurations.

First of all, you need to add the environment variables to conf/tajo-env.sh.

.. code-block:: bash

  # Hadoop home. Required
  export HADOOP_HOME= ...

  # The java implementation to use.  Required.
  export JAVA_HOME= ...

To launch the tajo master, execute start-tajo.sh.

.. code-block:: bash

  $ $TAJO_HOME/bin/start-tajo.sh

.. note::

  If you want to how to setup a fully distributed mode of Tajo, please see :doc:`/configuration/cluster_setup`.

.. warning::

  By default, *Catalog server* which manages table meta data uses `Apache Derby <http://db.apache.org/derby/>`_ as a persistent storage, and Derby stores data into ``/tmp/tajo-catalog-${username}`` directory. But, some operating systems may remove all contents in ``/tmp`` when booting up. In order to ensure persistent store of your catalog data, you need to set a proper location of derby directory. To learn Catalog configuration, please refer to :doc:`/configuration/catalog_configuration`.


