*********************************
High Availability for TajoMaster
*********************************

TajoMaster is a Single Point of Failure in a Tajo Cluster because TajoMaster is the central controlling entity for all components of the Tajo system. TajoMaster failure prevents clients from submitting new queries to the cluster, and results in the disruption of the ability to run insert overwrite queries because the TajoWorker can’t apply its statistical information to CatalogStore. Therefore, the high-availability (HA) of TajoMaster is essential for the high-availability of Tajo generally.

Currently, TajoMaster HA provides the following elements:

* Automatic failover of TajoMaster: Even if the active TajoMaster stops, the standby TajoMaster will become the active node.
* Preservation of the ongoing query in the cluster: Even if the active TajoMaster stops, the ongoing query will still complete in the cluster.


================================================
  Terminology
================================================

* Active master: TajoMaster that is actively serving the all operation from TajoClient and TajoWorker.
* Backup master: This TajoMaster waits becomes active when the Active dies or unhealthy. Users can setup multiple back TajoMaster, and this servers monitors the Active status to become active.


================================================
  Configuration File Settings
================================================

If you want to use TajoMaster HA mode, specific your ``tajo.master.ha.enable`` as follows:

.. code-block:: xml

  <property>
    <name>tajo.master.ha.enable</name>
    <value>true</value>
  </property>

If you use HA mode, all back masters monitor the active master at 5 second intervals. If you update this period, specific your ``tajo.master.ha.monitor.interval`` as follows:

.. code-block:: xml

  <property>
    <name>tajo.master.ha.monitor.interval</name>
    <value>monitor interval</value>
  </property>


================================================
  Backup Master Settings
================================================

If you want to run masters with ``start-tajo.sh``, specific your masters in ``conf/masters``. The file lists all host names of masters, one per line.By default, this file contains the single entry ``localhost``. You can easily add host names of workers via your favorite text editor.

For example: ::

  $ cat > conf/masters
  host1.domain.com
  host2.domain.com
  ....

  <ctrl + d>

And then, you need to setup tarball and set configuration files on backup masters.

.. note::

  If you want to run active master and backup master on the same host, you may find TajoMaster port conflicts. To avoid this problem, you must convert backup master primary ports to another port in ``tajo-site.xml`` as follows:

  .. code-block:: xml

    <property>
      <name>tajo.master.umbilical-rpc.address</name>
      <value>localhost:36001</value>
      <description>The default port is 26001.</description>
    </property>

    <property>
      <name>tajo.master.client-rpc.address</name>
      <value>localhost:36002</value>
      <description>The default port is 26002.</description>
    </property>

    <property>
      <name>tajo.resource-tracker.rpc.address</name>
      <value>localhost:36003</value>
      <description>The default port is 26003.</description>
      </property>

    <property>
      <name>tajo.catalog.client-rpc.address</name>
      <value>localhost:36005</value>
      <description>The default port is 26005.</description>
    </property>

    <property>
      <name>tajo.master.info-http.address</name>
      <value>0.0.0.0:36080</value>
      <description>The default port is 26080.</description>
    </property>


  And you need to convert ``TAJO_PID_DIR`` to another directory in ``tajo-env.sh``.


================================================
  Launch a Tajo cluster
================================================

Then, execute ``start-tajo.sh`` ::

  $ $TAJO_HOME/bin/start-tajo.sh

.. note::

  You can't use HA mode in DerbyStore. Currently, just one TajoMaster invoke the derby. If another master try to invoke it, it never run itself. Also, if you set another catalog uri for backup master, it is a incorrect configuration. Because they are unequal in every way.

================================================
  Administration HA state
================================================

If you want to transit any backup master to active master, execute ``tajo hadmin -transitionToActive`` ::

  $ $TAJO_HOME/bin/tajo haadmin -transitionToActive <target tajo.master.umbilical-rpc.address>

If you want to transit any active master to backup master, execute ``tajo hadmin -transitionToBackup`` ::

  $ $TAJO_HOME/bin/tajo haadmin -transitionToBackup <target tajo.master.umbilical-rpc.address>

If you want to find the state of any master, execute ``tajo hadmin -getState`` ::

  $ $TAJO_HOME/bin/tajo haadmin -getState <target tajo.master.umbilical-rpc.address>

If you want to initiate HA information, execute ``tajo haadmin -formatHA`` ::

  $ $TAJO_HOME/bin/tajo haadmin -formatHA

.. note::

  Before format HA, you must shutdown the Tajo cluster.


================================================
  How to Test Automatic Failover
================================================

If you want to verify automatic failover of TajoMaster, you must deploy your Tajo cluster with TajoMaster HA enable. And then, you need to find which node is active from Tajo web UI.

Once you find your active TajoMaster, you can cause a failure on that node. For example, you can use kill -9 <pid of TajoMaster> to simulate a JVM crash. Or you can shutdown the machine or disconnect network interface. And then, the backup TajoMaster will be automatically active within 5 seconds. The amount of time required to detect a failure and  trigger a failover depends on the config ``tajo.master.ha.monitor.interval``. If there is running queries, it will be finished successfully. Because your TajoClient will get the result data on TajoWorker. But you can't find already query history. Because TajoMaster stores query history on memory. So, the other master can't access already active master query history. And if there is no running query, the automatic failover run successfully.

For reference, TajoMaster HA doesn't consider TajoWorker failure. It is related with TajoResourceManager and QueryMaster.