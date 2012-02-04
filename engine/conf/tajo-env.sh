# Set Tazo-specific environment variables here.

# The only required environment variable is JAVA_HOME.  All others are
# optional.  When running a distributed configuration it is best to
# set JAVA_HOME in this file, so that it is correctly defined on
# remote nodes.

# The java implementation to use.  Required.
export JAVA_HOME=/usr

# Extra Java CLASSPATH elements.  Optional.
# export TAZO_CLASSPATH=

# The maximum amount of heap to use, in MB. Default is 1000.
# export TAZO_HEAPSIZE=2000

# Extra Java runtime options.  Empty by default.
# export TAZO_OPTS=-server

# Command specific options appended to TAZO_OPTS when specified
#export TAZO_NAMENODE_OPTS="-Dcom.sun.management.jmxremote $TAZO_NAMENODE_OPTS"
#export TAZO_SECONDARYNAMENODE_OPTS="-Dcom.sun.management.jmxremote $TAZO_SECONDARYNAMENODE_OPTS"
#export TAZO_DATANODE_OPTS="-Dcom.sun.management.jmxremote $TAZO_DATANODE_OPTS"
#export TAZO_BALANCER_OPTS="-Dcom.sun.management.jmxremote $TAZO_BALANCER_OPTS"
#export TAZO_JOBTRACKER_OPTS="-Dcom.sun.management.jmxremote $TAZO_JOBTRACKER_OPTS"

# export TAZO_TASKTRACKER_OPTS=
# The following applies to multiple commands (fs, dfs, fsck, distcp etc)
# export TAZO_CLIENT_OPTS

# Extra ssh options.  Empty by default.
# export TAZO_SSH_OPTS="-o ConnectTimeout=1 -o SendEnv=TAZO_CONF_DIR"

# Where log files are stored.  $TAZO_HOME/logs by default.
# export TAZO_LOG_DIR=${TAZO_HOME}/logs

# File naming remote slave hosts.  $TAZO_HOME/conf/slaves by default.
# export TAZO_SLAVES=${TAZO_HOME}/conf/slaves

# host:path where hadoop code should be rsync'd from.  Unset by default.
# export TAZO_MASTER=master:/home/$USER/src/hadoop

# Seconds to sleep between slave commands.  Unset by default.  This
# can be useful in large clusters, where, e.g., slave rsyncs can
# otherwise arrive faster than the master can service them.
# export TAZO_SLAVE_SLEEP=0.1

# The directory where pid files are stored. /tmp by default.
# export TAZO_PID_DIR=/var/hadoop/pids

# A string representing this instance of hadoop. $USER by default.
# export TAZO_IDENT_STRING=$USER

# The scheduling priority for daemon processes.  See 'man nice'.
# export TAZO_NICENESS=10
