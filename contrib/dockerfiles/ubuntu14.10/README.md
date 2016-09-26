<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
  -->

Tajo
====================

Components
----------
This is official Apache Tajo docker image.

* Ubuntu 14.10
* Hadoop 2.6.0
* Tajo 0.10.0

Run
---
You can build and run a cluster easily.

> $ sudo docker pull tajo/ubuntu14.10

> $ bash -c "$(curl -fsSL https://raw.githubusercontent.com/apache/tajo/master/dockerfiles/ubuntu14.10/run-cluster.sh) ubuntu14.10"

> ...

> $ root@hnn-001-01:~# ./init-tajo.sh

> $ root@hnn-001-01:~# start-tajo.sh

> $ root@hnn-001-01:~# ./test-tajo.sh

> ...

> $ root@hnn-001-01:~# tsql

> Try \? for help.

> default>

> ...

> $ root@hnn-001-01:~# exit
