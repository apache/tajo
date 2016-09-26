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

from rowdecoder import RowDecoder
from resultset import TajoResultSet

class TajoMemoryResultSet(TajoResultSet):
    def __init__(self, queryId, schema, serializedTuples):
        super(TajoMemoryResultSet, self).__init__()
        self.queryId = queryId
        self.schema = schema
        self.totalRow = len(serializedTuples)
        self.serializedTuples = serializedTuples
        self.decoder = RowDecoder(self.schema)
        self.cur = None

    def getQueryId(self):
        return self.queryId

    def nextTuple(self):
        if self.curRow < self.totalRow:
            self.cur = self.decoder.toTuple(self.serializedTuples[self.curRow])
            return self.cur

        return None
