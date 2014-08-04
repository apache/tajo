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


# Stop tajo master daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/tajo-config.sh

"$bin"/tajo-daemon.sh --config $TAJO_CONF_DIR stop master

if [ -f "${TAJO_CONF_DIR}/tajo-env.sh" ]; then
  . "${TAJO_CONF_DIR}/tajo-env.sh"
fi

if [ "$TAJO_WORKER_STANDBY_MODE" = "true" ]; then
  "$bin/tajo-daemons.sh" cd "$TAJO_HOME" \; "$bin/tajo-daemon.sh" stop worker
  if [ "$TAJO_PULLSERVER_MODE" = "dedicated" ]; then
    "$bin/tajo-daemons.sh" cd "$TAJO_HOME" \; "$bin/tajo-daemon.sh" stop pullserver
  fi
  if [ -f "${TAJO_CONF_DIR}/querymasters" ]; then
    "$bin/tajo-daemons.sh" --hosts querymasters cd "$TAJO_HOME" \; "$bin/tajo-daemon.sh" stop querymaster
  fi
fi

