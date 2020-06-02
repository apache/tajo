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

from tajo.error import *
from tajo.py3 import httplib, PY3

try:
    import simplejson as json
except ImportError:
    import json

class TajoObject(object):
    def __init__(self):
        pass

    @staticmethod
    def create(headers, content):
        raise NotImplementedMethodError("object create error")


class TajoRequest(object):
    object_cls = None
    ok_status = [httplib.OK]

    def __init__(self):
        pass

    def method(self):
        return "GET"

    def uri(self):
        raise NotImplementedMethodError("uri")

    def params(self):
        raise NotImplementedMethodError("params")

    def headers(self):
        raise NotImplementedMethodError("headers")

    def object_cls(self):
        if self.object_cls is None:
            raise NotImplementedMethodError("cls")

        return self.object_cls

    def check_status(self, headers, contents):
        status = int(headers["status"])
        if status not in self.ok_status:
            msg = status
            if PY3:
                contents = contents.decode('utf-8')

            c = json.loads(contents)
            if 'message' in c:
                msg = "%s %s"%(status, c["message"])

            if headers["status"][0] == '4':
                raise InvalidRequestError(msg)
            if headers["status"][0] == '5':
                raise InternalError(msg)

    def request(self, conn):
        headers, contents = conn._request(self.method(), self.uri(), self.params())
        self.check_status(headers, contents)
        return self.object_cls.create(headers, contents)


class TajoPostRequest(TajoRequest):
    def method(self):
        return "POST"

class TajoDeleteRequest(TajoRequest):
    def method(self):
        return "DELETE"
