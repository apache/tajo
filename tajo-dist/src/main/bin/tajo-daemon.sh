#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Runs a Tajo command as a daemon.
#
# Environment Variables
#
#   TAJO_CONF_DIR  Alternate conf dir. Default is ${TAJO_HOME}/conf.
#   TAJO_LOG_DIR   Where log files are stored.  PWD by default.
#   TAJO_MASTER    host:path where tajo code should be rsync'd from
#   TAJO_PID_DIR   The pid files are stored. /tmp by default.
#   TAJO_IDENT_STRING   A string representing this instance of tajo. $USER by default
#   TAJO_NICENESS The scheduling priority for daemons. Defaults to 0.
##

usage="Usage: tajo-daemon.sh [--config <conf-dir>] [--hosts hostlistfile] (start|stop) <tajo-command> <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/tajo-config.sh

# get arguments
startStop=$1
shift
command=$1
shift

tajo_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}

if [ -f "${TAJO_CONF_DIR}/tajo-env.sh" ]; then
  . "${TAJO_CONF_DIR}/tajo-env.sh"
fi

# Determine if we're starting a secure server, and if so, redefine appropriate variables
if [ "$command" == "worker" ] && [ "$EUID" -eq 0 ] && [ -n "$TAJO_SECURE_WORKER_USER" ]; then
  export TAJO_PID_DIR=$TAJO_SECURE_WORKER_PID_DIR
  export TAJO_LOG_DIR=$TAJO_SECURE_WORKER_LOG_DIR
  export TAJO_IDENT_STRING=$TAJO_SECURE_WORKER_USER
fi

if [ "$TAJO_IDENT_STRING" = "" ]; then
  export TAJO_IDENT_STRING="$USER"
fi

# get log directory
if [ "$TAJO_LOG_DIR" = "" ]; then
  export TAJO_LOG_DIR="$TAJO_HOME/logs"
fi
mkdir -p "$TAJO_LOG_DIR"
chown $TAJO_IDENT_STRING $TAJO_LOG_DIR 

if [ "$TAJO_PID_DIR" = "" ]; then
  TAJO_PID_DIR=/tmp
fi

# some variables
export TAJO_LOGFILE=tajo-$TAJO_IDENT_STRING-$command-$HOSTNAME.log
export TAJO_ROOT_LOGGER_APPENDER="${TAJO_ROOT_LOGGER_APPENDER:-DRFA}"
export TAJO_PULLSERVER_STANDALONE="${TAJO_PULLSERVER_STANDALONE:-false}"
log=$TAJO_LOG_DIR/tajo-$TAJO_IDENT_STRING-$command-$HOSTNAME.out
pid=$TAJO_PID_DIR/tajo-$TAJO_IDENT_STRING-$command.pid

# Set default scheduling priority
if [ "$TAJO_NICENESS" = "" ]; then
    export TAJO_NICENESS=0
fi

case $startStop in

  (start)

    mkdir -p "$TAJO_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    if [ "$TAJO_MASTER" != "" ]; then
      echo rsync from $TAJO_MASTER
      rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' $TAJO_MASTER/ "$TAJO_HOME"
    fi

    tajo_rotate_log $log
    echo starting $command, logging to $log
    cd "$TAJO_HOME"
    nohup nice -n $TAJO_NICENESS "$TAJO_HOME"/bin/tajo --config $TAJO_CONF_DIR $command "$@" > "$log" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head "$log"
    ;;
          
  (stop)

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo stopping $command
        kill `cat $pid`
      else
        echo no $command to stop
      fi
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac


