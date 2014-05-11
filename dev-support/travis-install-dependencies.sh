#!/bin/bash -x

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

PROTOBUF_VERSION=2.5.0
INSTALL=${HOME}/local

if [ ! -d ${INSTALL} ]; then
  echo "mkdir -p ${INSTALL}"
  mkdir -p ${INSTALL}
fi

if [ ! -f ${INSTALL}/bin/protoc ]; then
    cd ${INSTALL}
    echo "Fetching protobuf"
    N="protobuf-${PROTOBUF_VERSION}"
    wget -q https://protobuf.googlecode.com/files/${N}.tar.gz
    tar -xzvf ${N}.tar.gz > /dev/null
    rm ${N}.tar.gz

    echo "Building protobuf"
    cd ${N}
    ./configure --with-pic --prefix=${INSTALL} --with-gflags=${INSTALL} > /dev/null
    make -j4 install > /dev/null
fi
