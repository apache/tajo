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


# Start tajo map reduce daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/tajo-config.sh

# start the tajo master daemon
AUTOHA_ENABLED=$("$bin"/tajo getconf tajo.master.ha.enable)

if [ "$AUTOHA_ENABLED" = "true" ]; then
  echo "Starting TajoMasters on HA mode"
  if [ -f "${TAJO_CONF_DIR}/masters" ]; then
    MASTER_FILE=${TAJO_CONF_DIR}/masters
    MASTER_NAMES=$(cat "$MASTER_FILE" | sed  's/#.*$//;/^$/d')
    "$bin/tajo-daemons.sh" --hosts masters cd "$TAJO_HOME" \; "$bin/tajo-daemon.sh" --config $TAJO_CONF_DIR start master
  fi
else
  echo "Starting single TajoMaster"
  "$bin"/tajo-daemon.sh --config $TAJO_CONF_DIR start master
fi


if [ -f "${TAJO_CONF_DIR}/tajo-env.sh" ]; then
  . "${TAJO_CONF_DIR}/tajo-env.sh"
fi


if [ "$TAJO_PULLSERVER_STANDALONE" = "true" ]; then
  "$bin/tajo-daemons.sh" cd "$TAJO_HOME" \; "$bin/tajo-daemon.sh" --config $TAJO_CONF_DIR start pullserver
fi

"$bin/tajo-daemons.sh" cd "$TAJO_HOME" \; "$bin/tajo-daemon.sh" --config $TAJO_CONF_DIR start worker

# Display WEB UI URL and TajoMaster RPC address.
# Getting configuration value of http address and rpc address.
HTTP_ADDRESS=$("$bin"/tajo getconf tajo.master.info-http.address)
RPC_ADDRESS=$("$bin"/tajo getconf tajo.master.client-rpc.address)
HTTP_ADDRESS=(${HTTP_ADDRESS//:/ })
RPC_ADDRESS=(${RPC_ADDRESS//:/ })

if [ ${HTTP_ADDRESS[0]} = "0.0.0.0" ] ||
[ ${HTTP_ADDRESS[0]} = "127.0.0.1" ] ||
[ ${HTTP_ADDRESS[0]} = "localhost" ]; then
  HTTP_ADDRESS[0]=`hostname`
fi

if [ ${RPC_ADDRESS[0]} = "0.0.0.0" ] ||
[ ${RPC_ADDRESS[0]} = "127.0.0.1" ] ||
[ ${RPC_ADDRESS[0]} = "localhost" ]; then
  RPC_ADDRESS[0]=`hostname`
fi

echo "Tajo master web UI: http://${HTTP_ADDRESS[0]}:${HTTP_ADDRESS[1]}"
echo "Tajo Client Service: ${RPC_ADDRESS[0]}:${RPC_ADDRESS[1]}"
