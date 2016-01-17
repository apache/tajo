*************************************
HBase Integration
*************************************

Apache Tajo™ storage supports integration with Apache HBase™.
This integration allows Tajo to access all tables used in Apache HBase.

In order to use this feature, you need to build add some configs into ``conf/tajo-env.sh`` and then add some properties into a table create statement.

This section describes how to setup HBase integration.

First, you need to set your HBase home directory to the environment variable ``HBASE_HOME`` in ``conf/tajo-env.sh`` as follows: ::

  export HBASE_HOME=/path/to/your/hbase/directory

If you set the directory, Tajo will add HBase library file to classpath.

Next, you must configure tablespace about HBase. Please see :doc:`/table_management/tablespaces` if you want to know more information about it.



========================
CREATE TABLE
========================

*Synopsis*

.. code-block:: sql

  CREATE [EXTERNAL] TABLE [IF NOT EXISTS] <table_name> [(<column_name> <data_type>, ... )]
  USING hbase
  WITH ('table'='<hbase_table_name>'
  , 'columns'=':key,<column_family_name>:<qualifier_name>, ...'
  , 'hbase.zookeeper.quorum'='<zookeeper_address>'
  , 'hbase.zookeeper.property.clientPort'='<zookeeper_client_port>')
  [LOCATION 'hbase:zk://<hostname>:<port>/'] ;

``IF NOT EXISTS`` allows ``CREATE [EXTERNAL] TABLE`` statement to avoid an error which occurs when the table does not exist.

If you want to create ``EXTERNAL TABLE``, You must write ``LOCATION`` statement.

Options

* ``table`` : Set hbase origin table name. If you want to create an external table, the table must exists on HBase. The other way, if you want to create a managed table, the table must doesn't exist on HBase.
* ``columns`` : :key means HBase row key. The number of columns entry need to equals to the number of Tajo table column
* ``hbase.zookeeper.quorum`` : Set zookeeper quorum address. You can use different zookeeper cluster on the same Tajo database. If you don't set the zookeeper address, Tajo will refer the property of hbase-site.xml file.
* ``hbase.zookeeper.property.clientPort`` : Set zookeeper client port. If you don't set the port, Tajo will refer the property of hbase-site.xml file.




========================
 DROP TABLE
========================

*Synopsis*

.. code-block:: sql

  DROP TABLE [IF EXISTS] <table_name> [PURGE]

``IF EXISTS`` allows ``DROP TABLE`` statement to avoid an error which occurs when the table does not exist. ``DROP TABLE`` statement removes a table from Tajo catalog, but it does not remove the contents on HBase cluster. If ``PURGE`` option is given, ``DROP TABLE`` statement will eliminate the entry in the catalog as well as the contents on HBase cluster.


========================
INSERT (OVERWRITE) INTO
========================

INSERT OVERWRITE statement overwrites a table data of an existing table. Tajo's INSERT OVERWRITE statement follows ``INSERT INTO SELECT`` statement of SQL. The examples are as follows:

.. code-block:: sql

  -- when a target table schema and output schema are equivalent to each other
  INSERT OVERWRITE INTO t1 SELECT l_orderkey, l_partkey, l_quantity FROM lineitem;
  -- or
  INSERT OVERWRITE INTO t1 SELECT * FROM lineitem;

  -- when the output schema are smaller than the target table schema
  INSERT OVERWRITE INTO t1 SELECT l_orderkey FROM lineitem;

  -- when you want to specify certain target columns
  INSERT OVERWRITE INTO t1 (col1, col3) SELECT l_orderkey, l_quantity FROM lineitem;


.. note::

  If you don't set row key option, You are never able to use your table data. Because Tajo need to have some key columns for sorting before creating result data.



========================
Usage
========================

In order to create a new HBase table which is to be managed by Tajo, use the USING clause on CREATE TABLE:

.. code-block:: sql

  CREATE EXTERNAL TABLE blog (rowkey text, author text, register_date text, title text)
  USING hbase WITH (
    'table'='blog'
    , 'columns'=':key,info:author,info:date,content:title')
  LOCATION 'hbase:zk://<hostname>:<port>/';

After executing the command above, you should be able to see the new table in the HBase shell:

.. code-block:: sql

  $ hbase shell
  create 'blog', {NAME=>'info'}, {NAME=>'content'}
  put 'blog', 'hyunsik-02', 'content:title', 'Getting started with Tajo on your desktop'
  put 'blog', 'hyunsik-02', 'info:author', 'Hyunsik Choi'
  put 'blog', 'hyunsik-02', 'info:date', '2014-12-03'
  put 'blog', 'blrunner-01', 'content:title', 'Apache Tajo: A Big Data Warehouse System on Hadoop'
  put 'blog', 'blrunner-01', 'info:author', 'Jaehwa Jung'
  put 'blog', 'blrunner-01', 'info:date', '2014-10-31'
  put 'blog', 'jhkim-01', 'content:title', 'APACHE TAJO™ v0.9 HAS ARRIVED!'
  put 'blog', 'jhkim-01', 'info:author', 'Jinho Kim'
  put 'blog', 'jhkim-01', 'info:date', '2014-10-22'

And then create the table and query the table meta data with ``\d`` option:

.. code-block:: sql

  default> \d blog;

  table name: default.blog
  table path:
  store type: HBASE
  number of rows: unknown
  volume: 0 B
  Options:
          'columns'=':key,info:author,info:date,content:title'
          'table'='blog'

  schema:
  rowkey  TEXT
  author  TEXT
  register_date   TEXT
  title   TEXT


And then query the table as follows:

.. code-block:: sql

  default> SELECT * FROM blog;
  rowkey,  author,  register_date,  title
  -------------------------------
  blrunner-01,  Jaehwa Jung,  2014-10-31,  Apache Tajo: A Big Data Warehouse System on Hadoop
  hyunsik-02,  Hyunsik Choi,  2014-12-03,  Getting started with Tajo on your desktop
  jhkim-01,  Jinho Kim,  2014-10-22,  APACHE TAJO™ v0.9 HAS ARRIVED!

  default> SELECT * FROM blog WHERE rowkey = 'blrunner-01';
  Progress: 100%, response time: 2.043 sec
  rowkey,  author,  register_date,  title
  -------------------------------
  blrunner-01,  Jaehwa Jung,  2014-10-31,  Apache Tajo: A Big Data Warehouse System on Hadoop


Here's how to insert data the HBase table:

.. code-block:: sql

  CREATE TABLE blog_backup(rowkey text, author text, register_date text, title text)
  USING hbase WITH (
    'table'='blog_backup'
    , 'columns'=':key,info:author,info:date,content:title');
  INSERT OVERWRITE INTO blog_backup SELECT * FROM blog;


Use HBase shell to verify that the data actually got loaded:

.. code-block:: sql

  hbase(main):004:0> scan 'blog_backup'
   ROW          COLUMN+CELL
   blrunner-01  column=content:title, timestamp=1421227531054, value=Apache Tajo: A Big Data Warehouse System on Hadoop
   blrunner-01  column=info:author, timestamp=1421227531054, value=Jaehwa Jung
   blrunner-01  column=info:date, timestamp=1421227531054, value=2014-10-31
   hyunsik-02   column=content:title, timestamp=1421227531054, value=Getting started with Tajo on your desktop
   hyunsik-02   column=info:author, timestamp=1421227531054, value=Hyunsik Choi
   hyunsik-02   column=info:date, timestamp=1421227531054, value=2014-12-03
   jhkim-01     column=content:title, timestamp=1421227531054, value=APACHE TAJO\xE2\x84\xA2 v0.9 HAS ARRIVED!
   jhkim-01     column=info:author, timestamp=1421227531054, value=Jinho Kim
   jhkim-01     column=info:date, timestamp=1421227531054, value=2014-10-22
  3 row(s) in 0.0470 seconds


