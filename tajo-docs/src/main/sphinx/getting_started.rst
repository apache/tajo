***************
Getting Started
***************

In this section, we explain setup of a standalone Tajo instance. It will run against the local filesystem. In later sections, we will present how to run Tajo cluster instance on Apache Hadoop's HDFS, a distributed filesystem. This section shows you how to start up a Tajo cluster, create tables in your Tajo cluster, submit SQL queries via Tajo shell, and shutting down your Tajo cluster instance. The below exercise should take no more than ten minutes.

======================
Prerequisites
======================

 * Hadoop 2.3.0 or higher (up to 2.6.0)
 * Java 1.6 or 1.7
 * Protocol buffer 2.5.0

===================================
Dowload and unpack the source code
===================================

You can either download the source code release of Tajo or check out the development codebase from Git.

-----------------------------------
Download the latest source release
-----------------------------------

Choose a download site from this list of `Apache Download Mirrors <http://www.apache.org/dyn/closer.cgi/tajo>`_.
Click on the suggested mirror link. This will take you to a mirror of Tajo Releases. 
Download the file that ends in .tar.gz to your local filesystem, e.g. tajo-x.y.z-src.tar.gz.

Decompress and untar your downloaded file and then change into the unpacked directory. ::

  tar xzvf tajo-x.y.z-src.tar.gz

-----------------------------------
Check out the source code via Git
-----------------------------------

The development codebase can also be downloaded from `the Apache git repository <https://git-wip-us.apache.org/repos/asf/tajo.git>`_ as follows: ::

  git clone https://git-wip-us.apache.org/repos/asf/tajo.git

A read-only git repository is also mirrored on `Github <https://github.com/apache/tajo>`_.


=================
Build source code
=================

You prepare the prerequisites and the source code, you can build the source code now.

The first step of the installation procedure is to configure the source tree for your system and choose the options you would like. This is done by running the configure script. For a default installation simply enter:

You can compile source code and get a binary archive as follows:

.. code-block:: bash

  $ cd tajo-x.y.z
  $ mvn clean install -DskipTests -Pdist -Dtar -Dhadoop.version=2.X.X
  $ ls tajo-dist/target/tajo-x.y.z-SNAPSHOT.tar.gz

.. note::

  If you don't specify the hadoop version, tajo cluster may not run correctly. Thus, we highly recommend that you specify your hadoop version with maven build command.

  Example:

    $ mvn clean install -DskipTests -Pdist -Dtar -Dhadoop.version=2.5.1

Then, after you move some proper directory, discompress the tar.gz file as follows:

.. code-block:: bash

  $ cd [a directory to be parent of tajo binary]
  $ tar xzvf ${TAJO_SRC}/tajo-dist/target/tajo-x.y.z-SNAPSHOT.tar.gz

================================
Setting up a local Tajo cluster
================================

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

======================
First query execution
======================

First of all, we need to prepare some table for query execution. For example, you can make a simple text-based table as follows: 

.. code-block:: bash

  $ mkdir /home/x/table1
  $ cd /home/x/table1
  $ cat > data.csv
  1|abc|1.1|a
  2|def|2.3|b
  3|ghi|3.4|c
  4|jkl|4.5|d
  5|mno|5.6|e
  <CTRL + D>


Apache Tajo™ provides a SQL shell which allows users to interactively submit SQL queries. In order to use this shell, please execute ``bin/tsql`` ::

  $ $TAJO_HOME/bin/tsql
  tajo>

In order to load the table we created above, we should think of a schema of the table.
Here, we assume the schema as (int, text, float, text). ::

  $ $TAJO_HOME/bin/tsql
  tajo> create external table table1 (
        id int,
        name text, 
        score float, 
        type text) 
        using text with ('text.delimiter'='|') location 'file:/home/x/table1';

To load an external table, you need to use ‘create external table’ statement. 
In the location clause, you should use the absolute directory path with an appropriate scheme. 
If the table resides in HDFS, you should use ‘hdfs’ instead of ‘file’.

If you want to know DDL statements in more detail, please see Query Language. ::

  tajo> \d
  table1

 ``\d`` command shows the list of tables. ::

  tajo> \d table1

  table name: table1
  table path: file:/home/x/table1
  store type: CSV
  number of rows: 0
  volume (bytes): 78 B
  schema:
  id      INT
  name    TEXT
  score   FLOAT
  type    TEXT

``\d [table name]`` command shows the description of a given table.

Also, you can execute SQL queries as follows: ::

  tajo> select * from table1 where id > 2;
  final state: QUERY_SUCCEEDED, init time: 0.069 sec, response time: 0.397 sec
  result: file:/tmp/tajo-hadoop/staging/q_1363768615503_0001_000001/RESULT, 3 rows ( 35B)

  id,  name,  score,  type
  - - - - - - - - - -  - - -
  3,  ghi,  3.4,  c
  4,  jkl,  4.5,  d
  5,  mno,  5.6,  e

  tajo> \q
  bye

Feel free to enjoy Tajo with SQL standards. 
If you want to know more explanation for SQL supported by Tajo, please refer :doc:`/sql_language`.