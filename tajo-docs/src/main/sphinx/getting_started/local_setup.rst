**********************************
Setting up a local Tajo cluster
**********************************

First of all, you need to add the environment variables to conf/tajo-env.sh. ::

  # Hadoop home. Required
  export HADOOP_HOME= ...

  # The java implementation to use.  Required.
  export JAVA_HOME= ...

To launch the tajo master, execute start-tajo.sh. ::

  $ $TAJO_HOME/bin/start-tajo.sh

After then, you can use tsql, which is the command line shell of Tajo. ::

  $ $TAJO_HOME/bin/tsql
  tajo>

If you want to how to use tsql, read Tajo Interactive Shell document.