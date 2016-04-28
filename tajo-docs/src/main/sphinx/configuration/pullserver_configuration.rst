*************************
Pull Server Configuration
*************************

Pull servers are responsible for transmitting data among Tajo workers during shuffle phases. Tajo provides several modes
for pull servers.

Internal Mode (Default)
=======================

With the internal mode, each worker acts as a pull server. So, they need to transmit data during shuffle phase as well
as processing them during processing phase.

Standalone Mode
===============

Sometimes, data shuffling requires huge memory space and a lot of cpu processing.
This can make query processing slow because Tajo's query engine should contend for limited resources with pull servers.
Tajo provides the standalone mode to avoid this unnecessary contention.

In this mode, each pull server is executed as a separate process. To enable this mode, you need to add the following
line to ``${TAJO_HOME}/conf/tajo-env.sh``.

.. code-block:: sh

  export TAJO_PULLSERVER_STANDALONE=true

Then, you can see the following messages when you start up the tajo cluster.

.. code-block:: sh

  Starting single TajoMaster
  starting master, logging to ...
  192.168.10.1: starting pullserver, logging to ...
  192.168.10.1: starting worker, logging to ...
  192.168.10.2: starting pullserver, logging to ...
  192.168.10.2: starting worker, logging to ...
  ...

.. warning::

  Currently, only one single server should be run in each machine.

Yarn Auxiliary Service Mode
===========================

You can run pull servers as one of Yarn's auxiliary services. To do so, you need to add the following configurations
to ``${HADOOP_CONF}/yarn-site.xml``.

.. code-block:: xml

  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle,tajo_shuffle</value>
  </property>

  <property>
    <name>yarn.nodemanager.aux-services.tajo_shuffle.class</name>
    <value>org.apache.tajo.yarn.TajoPullServerService</value>
  </property>

  <property>
    <name>tajo.pullserver.port</name>
    <value>port number</value>
  </property>

Optionally, you can add the below configuration to specify temp directories. For this configuration,
please refer to :doc:`/configuration/worker_configuration`.

.. code-block:: xml

  <property>
    <name>tajo.worker.tmpdir.locations</name>
    <value>/path/to/tajo/temporal/directory</value>
  </property>