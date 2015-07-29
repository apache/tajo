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

from tajo.client import TajoClient

client = TajoClient("http://127.0.0.1:26880/rest", username='tajo')

client.execute_query_wait_result('create table Test1 (col1 int)')
client.execute_query_wait_result('create table Test2 (col1 int)')
client.execute_query_wait_result('create table Test3 (col1 int)')
client.execute_query_wait_result('create table Test4 (col1 int)')

tables = client.tables("default")
print(tables)
for table in tables:
    t = client.table("default", table.name)
    print(t)

client.execute_query_wait_result('drop table Test1 purge')
client.execute_query_wait_result('drop table Test2 purge')
client.execute_query_wait_result('drop table Test3 purge')
client.execute_query_wait_result('drop table Test4 purge')
