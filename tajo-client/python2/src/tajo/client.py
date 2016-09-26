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

from connection import TajoSessionConnection
from queryid import QueryId
from memoryresultset import TajoMemoryResultSet
from fetchresultset import TajoFetchResultSet

import proto.ClientProtos_pb2 as ClientProtos_pb2
import proto.CatalogProtos_pb2 as CatalogProtos_pb2
from querystate import QueryState

import time

OK_VALUE = 0
ERROR_VALUE = 1

FETCH_ROW_NUM = 10

class TajoClient(TajoSessionConnection, object):
    def __init__(self, host, port, username='tmpuser'):
        super(TajoClient, self).__init__(host, port, username)

    def executeQuery(self, sql):
        request = ClientProtos_pb2.QueryRequest()
        request.sessionId.id = self.checkSessionAndGet()
        request.query = sql
        request.isJson = False
        return self.service.submitQuery(request)

    def getQueryStatus(self, queryId):
        request = ClientProtos_pb2.GetQueryStatusRequest()
        request.sessionId.id = self.checkSessionAndGet()
        request.queryId.id = queryId.id
        request.queryId.seq = queryId.seq
        return self.service.getQueryStatus(request)

    def createNullResultSet(self, queryId):
        return TajoMemoryResultSet(queryId, CatalogProtos_pb2.SchemaProto(), [])

    def isNullQueryId(self, queryId):
        return queryId == QueryId.NULL_QUERY_ID

    def getLogicalSchema(self, tableDesc):
        schema = tableDesc.schema
        if tableDesc.HasField('partition'):
            partition = tableDesc.partition
            start = len(schema.fields)
            count = 0
            for field in partition.expressionSchema.fields:
                new_field = schema.fields.add()
                new_field.tid = field.tid
                new_field.name = field.name
                new_field.dataType.type = field.dataType.type
                new_field.dataType.code = field.dataType.code
                new_field.dataType.length = field.dataType.length

        return schema

    def createResultSet(self, response, fetchRowNum):
        if response.HasField('tableDesc'):
            return TajoFetchResultSet(self, response.queryId,
                    self.getLogicalSchema(response.tableDesc), FETCH_ROW_NUM)
        else:
            return TajoMemoryResultSet(response.queryId, response.resultSet.schema,
                                       response.resultSet.serializedTuples)

    def executeQueryAndGetResult(self, sql):
        response = self.executeQuery(sql)
        if self.isError(response.resultCode):
            raise Exception(response.errorMessage + " " + response.errorTrace)

        queryId = response.queryId
        if response.isForwarded is True:
            if self.isNullQueryId(queryId.id):
                return self.createNullResultSet(queryId)
            else:
                return self.getQueryResultAndWait(queryId)

        if self.isNullQueryId(queryId.id) and response.maxRowNum == 0:
            return self.createNullResultSet(queryId)

        if response.HasField('resultSet') == False and response.HasField('tableDesc') == False:
            return self.createNullResultSet(queryId)

        return self.createResultSet(response, 10)

    def isError(self, code):
        return code == ERROR_VALUE

    def fetchNextQueryResult(self, queryId, schema, fetchRowNum):
        request = ClientProtos_pb2.GetQueryResultDataRequest()
        request.sessionId.id = self.checkSessionAndGet()
        request.queryId.id = queryId.id
        request.queryId.seq = queryId.seq
        request.fetchRowNum = fetchRowNum
        response = self.service.getQueryResultData(request)
        if self.isError(response.resultCode):
            raise Exception(response.errorMessage + " " + response.errorTrace)

        return TajoMemoryResultSet(queryId, schema,
                                   response.resultSet.serializedTuples)

    def isQueryWaitingForSchedule(self, state):
        return state == QueryState.QUERY_NOT_ASSIGNED or \
                state == QueryState.QUERY_MASTER_INIT or \
                state == QueryState.QUERY_MASTER_LAUNCHED

    def isQueryInited(self, state):
        return state == QueryState.QUERY_NEW

    def isQueryRunning(self, state):
        return self.isQueryInited(state) or (state == QueryState.QUERY_RUNNING)

    def isQueryComplete(self, state):
        return self.isQueryWaitingForSchedule(state) == False and \
               self.isQueryRunning(state) == False

    def getQueryResultAndWait(self, queryId):
        if self.isNullQueryId(queryId.id):
            return self.createNullResultSet(queryId)

        status = self.getQueryStatus(queryId)
        while (status != None and self.isQueryComplete(status.state) == False):
            time.sleep(1)
            status = self.getQueryStatus(queryId)

        if status.state == QueryState.QUERY_SUCCEEDED:
            if status.hasResult:
                return self.getQueryResult(queryId);

        return self.createNullResultSet(queryId);

    def getQueryResult(self, queryId):
        if self.isNullQueryId(queryId):
            return self.createNullResultSet(queryId)

        response = self.getResultResponse(queryId)
        return TajoFetchResultSet(self, queryId,
                self.getLogicalSchema(response.tableDesc), FETCH_ROW_NUM)

    def getResultResponse(self, queryId):
        request = ClientProtos_pb2.GetQueryResultRequest()
        request.sessionId.id = self.checkSessionAndGet()
        request.queryId.id = queryId.id
        request.queryId.seq = queryId.seq
        return self.service.getQueryResult(request)
