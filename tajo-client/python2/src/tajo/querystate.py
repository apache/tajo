# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

class QueryState:
    QUERY_MASTER_INIT = 0
    QUERY_MASTER_LAUNCHED = 1
    QUERY_NEW = 2
    QUERY_INIT = 3
    QUERY_RUNNING = 4
    QUERY_SUCCEEDED = 5
    QUERY_FAILED = 6
    QUERY_KILLED = 7
    QUERY_KILL_WAIT = 8
    QUERY_ERROR = 9
    QUERY_NOT_ASSIGNED = 10
