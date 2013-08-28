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

# Run a shell command on all worker hosts.
#
# Environment Variables
#
#   TAJO_WORKERS    File naming remote hosts.
#     Default is ${TAJO_CONF_DIR}/workers.
#   TAJO_CONF_DIR  Alternate conf dir. Default is ${TAJO_HOME}/conf.
#   TAJO_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   TAJO_SSH_OPTS Options passed to ssh when running remote commands.
##

usage="Usage: tajo-daemons.sh command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/tajo-config.sh

# If the workers file is specified in the command line,
# then it takes precedence over the definition in
# neptune-env.sh. Save it here.
HOSTLIST=$TAJO_WORKERS

if [ -f "${TAJO_CONF_DIR}/tajo-env.sh" ]; then
  . "${TAJO_CONF_DIR}/tajo-env.sh"
fi

if [ "$HOSTLIST" = "" ]; then
  if [ "$TAJO_WORKERS" = "" ]; then
    export HOSTLIST="${TAJO_CONF_DIR}/workers"
  else
    export HOSTLIST="${TAJO_CONF_DIR}"
  fi
fi

for slave in `cat "$HOSTLIST"`; do
 ssh $TAJO_SSH_OPTS $slave $"${@// /\\ }" \
   2>&1 | sed "s/^/$slave: /" &
 if [ "$TAJO_SLAVE_SLEEP" != "" ]; then
   sleep $TAJO_SLAVE_SLEEP
 fi
done

wait
