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

from tajo.error import InvalidStatusError
from tajo.base import TajoPostRequest, TajoObject
from tajo.querystate import QueryState
from tajo.queryid import QueryId
from tajo.py3 import httplib, PY3

try:
    import simplejson as json
except ImportError:
    import json

class TajoQuery(TajoObject):
    def __init__(self, headers, contents=None):
        if PY3:
            contents = contents.decode('utf-8')

        self.completed = False
        self.objs = json.loads(contents)
        if "uri" in self.objs:
            self.url = self.objs["uri"]
            self.query_id = self.get_parse_query_id(self.url)
        else:
            self.query_id = QueryId.NULL_QUERY_ID
            self.completed = True
            self.status = QueryState.QUERY_SUCCEEDED

    def get_query_id(self):
        return self.query_id

    def get_parse_query_id(self, url):
        parts = url.split('/')
        return parts[-1]

    def __repr__(self):
        return str(self.uri)

    @staticmethod
    def create(headers, contents):
        return TajoQuery(headers, contents)


class TajoQueryRequest(TajoPostRequest):
    object_cls = TajoQuery
    ok_status = [httplib.CREATED, httplib.OK]

    def __init__(self, query):
        self.query = query

    def uri(self):
        return "queries"

    def headers(self):
        return None

    def params(self):
        payload = {
            'query': self.query
        }

        return payload

    def cls(self):
        return self.object_cls
