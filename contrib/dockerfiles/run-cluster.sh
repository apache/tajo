#!/bin/bash
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

if [[ "${!#}" =~ ^(ubuntu14.10|centos6)$ ]]
then
  IMAGE=tajo/${!#}
else
  echo "Usage: run-cluster.sh (ubuntu14.10|centos6)"
  exit 0
fi

LINK=""
for i in {1..3}
do
  HOST=hdw-001-0$i
  LINK="$LINK --link=$HOST:$HOST"
  docker run --name=$HOST -h $HOST -p 1001$i:22 -p 1920$i:9200 -d $IMAGE /root/start.sh
done

HOST=hnn-001-01
PORT="-p 8088:8088 -p 8888:8888 -p 10000:10000 -p 10010:22 -p 26002:26002 -p 26080:26080 -p 50070:50070"
echo "docker run --name=$HOST -h $HOST $PORT $LINK -it --rm $IMAGE /root/init-nn.sh"
docker run --name=$HOST -h $HOST $PORT $LINK -it --rm $IMAGE /root/init-nn.sh

for i in {1..3}
do
  docker rm -f hdw-001-0$i
done
