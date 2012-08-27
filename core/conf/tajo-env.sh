# Set Tazo-specific environment variables here.

# The only required environment variable is JAVA_HOME.  All others are
# optional.  When running a distributed configuration it is best to
# set JAVA_HOME in this file, so that it is correctly defined on
# remote nodes.

# The java implementation to use.  Required.
export JAVA_HOME=/usr

# Extra Java CLASSPATH elements.  Optional.
# export TAJO_CLASSPATH=

# The maximum amount of heap to use, in MB. Default is 1000.
export TAJO_HEAPSIZE=12000

# Extra Java runtime options.  Empty by default.
# export TAJO_OPTS=-server

# Command specific options appended to TAJO_OPTS when specified
#export TAJO_NAMENODE_OPTS="-Dcom.sun.management.jmxremote $TAJO_NAMENODE_OPTS"
#export TAJO_SECONDARYNAMENODE_OPTS="-Dcom.sun.management.jmxremote $TAJO_SECONDARYNAMENODE_OPTS"
#export TAJO_DATANODE_OPTS="-Dcom.sun.management.jmxremote $TAJO_DATANODE_OPTS"
#export TAJO_BALANCER_OPTS="-Dcom.sun.management.jmxremote $TAJO_BALANCER_OPTS"
#export TAJO_JOBTRACKER_OPTS="-Dcom.sun.management.jmxremote $TAJO_JOBTRACKER_OPTS"

# export TAJO_TASKTRACKER_OPTS=
# The following applies to multiple commands (fs, dfs, fsck, distcp etc)
# export TAJO_CLIENT_OPTS

# Extra ssh options.  Empty by default.
# export TAJO_SSH_OPTS="-o ConnectTimeout=1 -o SendEnv=TAJO_CONF_DIR"

# Where log files are stored.  $TAJO_HOME/logs by default.
# export TAJO_LOG_DIR=${TAJO_HOME}/logs

# File naming remote slave hosts.  $TAJO_HOME/conf/slaves by default.
# export TAJO_SLAVES=${TAJO_HOME}/conf/slaves

# host:path where hadoop code should be rsync'd from.  Unset by default.
# export TAJO_MASTER=master:/home/$USER/src/hadoop

# Seconds to sleep between slave commands.  Unset by default.  This
# can be useful in large clusters, where, e.g., slave rsyncs can
# otherwise arrive faster than the master can service them.
# export TAJO_SLAVE_SLEEP=0.1

# The directory where pid files are stored. /tmp by default.
# export TAJO_PID_DIR=/var/hadoop/pids

# A string representing this instance of hadoop. $USER by default.
# export TAJO_IDENT_STRING=$USER

# The scheduling priority for daemon processes.  See 'man nice'.
# export TAJO_NICENESS=10
