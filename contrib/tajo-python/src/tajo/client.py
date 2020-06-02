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

from tajo.session import TajoSessionRequest, TajoSession
from tajo.query import TajoQueryRequest, TajoQuery
from tajo.queryid import QueryId
from tajo.queries import TajoQueriesRequest
from tajo.querystatus import TajoQueryStatusRequest, TajoQueryStatus
from tajo.resultsetinfo import TajoResultSetInfoRequest, TajoResultSetInfo
from tajo.resultset import TajoMemoryResultSetRequest, TajoMemoryResultSet
from tajo.fetchresultset import TajoFetchResultSet
from tajo.connection import TajoConnection
from tajo.cluster import TajoCluster, TajoClusterRequest
from tajo.querystate import QueryState
from tajo.database import TajoDatabasesRequest, TajoDatabases, TajoDatabaseRequest, TajoDatabase
from tajo.database import TajoCreateDatabaseRequest, TajoDeleteDatabaseRequest
from tajo.tables import TajoTablesRequest, TajoTableRequest
from tajo.functions import TajoFunctionsRequest
from tajo.error import *

import time

class TajoClient(object):
    def __init__(self, base = 'http://127.0.0.1:26880/rest/',
                 username = 'tajo', database = 'default',
                 rowNum = 1024):
        self.clear()
        self.base = base
        self.username = username
        self.database = database
        self.rowNum = rowNum
        self.conn = TajoConnection(self.base)
        self.session = self.create_session()

    def clear(self):
        self.base = None
        self.username = "tajo"
        self.database = "default"
        self.rowNum = 1024
        self.conn = None
        self.session = None

    def create_session(self):
        request = TajoSessionRequest(self.username, self.database)
        self.session = request.request(self.conn)
        self.conn.add_header("X-Tajo-Session", str(self.session))

    def execute_query(self, query):
        req = TajoQueryRequest(query)
        return req.request(self.conn)

    def is_null_query_id(self, query_id):
        ret = False

        if query_id.query_id == QueryId.NULL_QUERY_ID:
            ret = True

        return ret

    def queries(self):
        req = TajoQueriesRequest(self.database)
        return req.request(self.conn)

    def query_status(self, query_id):
        if query_id.completed:
            return TajoQueryStatus(query_id.status)
        elif self.is_null_query_id(query_id):
            raise NullQueryIdError()

        req = TajoQueryStatusRequest(query_id, self.base)
        return req.request(self.conn)

    def query_resultset_info(self, query_id):
        if self.is_null_query_id(query_id):
            raise NullQueryIdError()

        if query_id.completed:
            return TajoResultSetInfo(None, query_id.schema)

        req = TajoResultSetInfoRequest(query_id, self.base)
        return req.request(self.conn)

    def query_resultset(self, resultsetinfo, count = 100):
        req = TajoMemoryResultSetRequest(resultsetinfo, self.base, count)
        return req.request(self.conn)

    def fetch(self, query_id, fetch_row_num = 100):
        resultset_info = self.query_resultset_info(query_id)
        return TajoFetchResultSet(self, query_id, resultset_info, fetch_row_num)

    def create_nullresultset(self, queryId):
        return TajoMemoryResultSet(None, True, 0, 0, None)

    def execute_query_wait_result(self, query):
        query_id = self.execute_query(query)
        if self.is_null_query_id(query_id):
            return self.create_nullresultset(query_id)

        status = self.query_status(query_id)
        while self.is_query_complete(status.state) == False:
            time.sleep(0.1)
            status = query_status(query_id)

        if status.state == QueryState.QUERY_SUCCEEDED:
            return self.fetch(query_id)

        return self.create_nullresultset(query_id)

    def is_query_waiting_for_schedule(self, state):
        return state == QueryState.QUERY_NOT_ASSIGNED or \
                state == QueryState.QUERY_MASTER_INIT or \
                state == QueryState.QUERY_MASTER_LAUNCHED

    def is_query_inited(self, state):
        return state == QueryState.QUERY_NEW

    def is_query_running(self, state):
        return self.is_query_inited(state) or (state == QueryState.QUERY_RUNNING)

    def is_query_complete(self, state):
        return self.is_query_waiting_for_schedule(state) == False and \
        self.is_query_running(state) == False

    def cluster_info(self):
        req = TajoClusterRequest()
        return req.request(self.conn)

    def databases(self):
        req = TajoDatabasesRequest()
        return req.request(self.conn)

    def database_info(self, database_name):
        req = TajoDatabaseRequest(database_name)
        return req.request(self.conn)

    def create_database(self, database_name):
        req = TajoCreateDatabaseRequest(database_name)
        return req.request(self.conn)

    def delete_database(self, database_name):
        req = TajoDeleteDatabaseRequest(database_name)
        return req.request(self.conn)

    def tables(self, database_name):
        req = TajoTablesRequest(database_name)
        return req.request(self.conn)

    def table(self, database_name, table_name):
        req = TajoTableRequest(database_name, table_name)
        return req.request(self.conn)

    def functions(self):
        req = TajoFunctionsRequest()
        return req.request(self.conn)
