*************************************
OpenStack Swift Integration
*************************************

Tajo supports OpenStack Swift as one of the underlying storage types.
In Tajo, Swift objects are represented and recognized by the same URI format as in Hadoop.

You don't need to run Hadoop to run Tajo on Swift, but need to configure it.
You will also need to configure Swift and Tajo.

For details, please see the following sections.

======================
Swift configuration
======================

This step is not mandatory, but is strongly recommended to configure the Swift's proxy-server with ``list_endpoints`` for better performance.
More information is available `here <http://docs.openstack.org/developer/swift/middleware.html#module-swift.common.middleware.list_endpoints>`_.

======================
Hadoop configurations
======================

You need to configure Hadoop to specify how to access Swift objects.
Here is an example of ``${HADOOP_HOME}/etc/hadoop/core-site.xml``.

-----------------------
Common configurations
-----------------------

.. code-block:: xml

  <property>
    <name>fs.swift.impl</name>
    <value>org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem</value>
    <description>File system implementation for Swift</description>
  </property>
  <property>
    <name>fs.swift.blocksize</name>
    <value>131072</value>
    <description>Split size in KB</description>
  </property>

----------------------------
Configurations per provider
----------------------------

.. code-block:: xml

  <property>
    <name>fs.swift.service.${PROVIDER}.auth.url</name>
    <value>http://127.0.0.1/v2.0/tokens</value>
    <description>Keystone authenticaiton URL</description>
  </property>
  <property>
    <name>fs.swift.service.${PROVIDER}.auth.endpoint.prefix</name>
    <value>/endpoints/AUTH_</value>
    <description>Keystone endpoints prefix</description>
  </property>
  <property>
    <name>fs.swift.service.${PROVIDER}.http.port</name>
    <value>8080</value>
    <description>HTTP port</description>
  </property>
  <property>
    <name>fs.swift.service.${PROVIDER}.region</name>
    <value>regionOne</value>
    <description>Region name</description>
  </property>
  <property>
    <name>fs.swift.service.${PROVIDER}.tenant</name>
    <value>demo</value>
    <description>Tenant name</description>
  </property>
  <property>
    <name>fs.swift.service.${PROVIDER}.username</name>
    <value>tajo</value>
  </property>
  <property>
    <name>fs.swift.service.${PROVIDER}.password</name>
    <value>tajo_password</value>
  </property>
  <property>
    <name>fs.swift.service.${PROVIDER}.location-aware</name>
    <value>true</value>
    <description>Flag to enable the location-aware computing</description>
  </property>

======================
Tajo configuration
======================

Finally, you need to configure the classpath of Tajo by adding the following line to ``${TAJO_HOME}/conf/tajo-evn.sh``.

.. code-block:: sh

  export TAJO_CLASSPATH=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-openstack-x.x.x.jar

======================
Querying on Swift
======================

Given a provider name *tajo* and a Swift container name *demo*, you can create a Tajo table with data on Swift as follows.

.. code-block:: sql

  default> create external table swift_table (id int32, name text, score float, type text) using text with ('text.delimiter'='|') location 'swift://demo.tajo/test.tbl';

Once a table is created, you can execute any SQL queries on that table as other tables stored on HDFS.
For query execution details, please refer to :doc:`sql_language`.