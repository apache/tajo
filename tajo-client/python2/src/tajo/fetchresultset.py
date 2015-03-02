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

from resultset import TajoResultSet

class TajoFetchResultSet(TajoResultSet):
    def __init__(self, client, queryId, schema, fetchRowNum):
        super(TajoFetchResultSet, self).__init__()
        self.queryId = queryId
        self.fetchRowNum = fetchRowNum
        self.client = client
        self.finished = False
        self.resultSet = None
        self.schema = schema

    def isFinished(self):
        return self.finished

    def getQueryId(self):
        return self.queryId

    def nextTuple(self):
        if self.isFinished() is True:
            return None

        t = None
        if self.resultSet is not None:
            self.resultSet.next()
            t = self.resultSet.getCurrentTuple()

        if self.resultSet is None or t is None:
            self.resultSet = self.client.fetchNextQueryResult(self.queryId, self.schema, self.fetchRowNum)
            if self.resultSet is None:
                self.finished = True
                return None

            self.resultSet.next()
            t = self.resultSet.getCurrentTuple()

        if t is None:
            if self.resultSet is not None:
                self.resultSet = None

            self.finished = True

        return t
