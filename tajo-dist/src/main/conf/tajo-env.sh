# Set Tajo-specific environment variables here.

# The only required environment variable is JAVA_HOME.  All others are
# optional.  When running a distributed configuration it is best to
# set JAVA_HOME in this file, so that it is correctly defined on
# remote nodes.

# The java implementation to use.  Required.
export JAVA_HOME=/usr

# Extra Java CLASSPATH elements.  Optional.
# export TAJO_CLASSPATH=

# The maximum amount of heap to use, in MB. Default is 1000.
# export TAJO_HEAPSIZE=1000

# Extra Java runtime options.  Empty by default.
# export TAJO_OPTS=-server
export TAJO_OPTS=-XX:+PrintGCTimeStamps

# Where log files are stored.  $TAJO_HOME/logs by default.
# export TAJO_LOG_DIR=${TAJO_HOME}/logs

# The directory where pid files are stored. /tmp by default.
# export TAJO_PID_DIR=/var/hadoop/pids

# A string representing this instance of hadoop. $USER by default.
# export TAJO_IDENT_STRING=$USER

# The scheduling priority for daemon processes.  See 'man nice'.
# export TAJO_NICENESS=10
