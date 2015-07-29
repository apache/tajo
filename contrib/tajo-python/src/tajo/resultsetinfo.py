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
from tajo.base import TajoRequest, TajoObject
from tajo.py3 import httplib, PY3

try:
    import simplejson as json
except ImportError:
    import json

class TajoResultSetInfo(TajoObject):
    def __init__(self, objs, schema=None):
        if objs is not None:
            self.objs = objs
            self.result_link = self.objs["resultset"]["link"]

        if objs is None and schema is not None:
            self.objs = { "schema": schema }
            self.result_link = ""

    def schema(self):
        return self.objs["schema"]

    def link(self):
        return self.result_link

    def __repr__(self):
        return self.result_link

    @staticmethod
    def create(headers, content):
        if PY3:
            content = content.decode('utf-8')

        objs = json.loads(content)
        if objs['resultCode'] != "OK":
            raise InvalidStatusError(int(headers['status']))

        return TajoResultSetInfo(objs)


class TajoResultSetInfoRequest(TajoRequest):
    object_cls = TajoResultSetInfo
    ok_status = [httplib.OK]

    def __init__(self, query_id, base):
        self.url = query_id.url
        self.base = base

    def uri(self):
        url = "%s/result"%(self.url[len(self.base):])
        return url

    def headers(self):
        return None

    def params(self):
        return None

    def cls(self):
        return self.object_cls
