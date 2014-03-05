***************
Preliminary
***************

===================================
catalog-site.xml and tajo-site.xml
===================================
Tajo's configuration is based on Hadoop's configuration system. Tajo uses two config files:

* catalog-site.xml - configuration for the catalog server.
* tajo-site.xml - configuration for other tajo modules. 

Each config consists of a pair of a name and a value. If you want to set the config name ``a.b.c`` with the value ``123``, add the following element to an appropriate file.

.. code-block:: xml

  <property>
    <name>a.b.c</name>
    <value>123</value>
  </property>

Tajo has a variety of internal configs. If you don't set some config explicitly, the default config will be used for for that config. Tajo is designed to use only a few of configs in usual cases. You may not be concerned with the configuration.

In default, there is no ``tajo-site.xml`` in ``${TAJO}/conf`` directory. If you set some configs, first copy ``$TAJO_HOME/conf/tajo-site.xml.templete`` to ``tajo-site.xml``. Then, add the configs to your tajo-site.

============
tajo-env.sh
============

tajo-env.sh is a shell script file. The main purpose of this file is to set shell environment variables for TajoMaster and TajoWorker java program. So, you can set some variable as follows:

.. code-block:: sh

  VARIABLE=value

If a value is a literal string, type this as follows:

.. code-block:: sh

  VARIABLE='value'