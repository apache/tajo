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

class TajoCluster(TajoObject):
    def __init__(self, objs):
        self.objs = objs

    def __repr__(self):
        return str(self.objs)

    @staticmethod
    def create(headers, content):
        if PY3:
            content = content.decode('utf-8')

        return TajoCluster(content)


class TajoClusterRequest(TajoRequest):
    object_cls = TajoCluster
    ok_status = [httplib.OK]

    def __init__(self):
        self.objs = None

    def uri(self):
        return "cluster"

    def headers(self):
        return None

    def params(self):
        return None

    def cls(self):
        return self.object_cls
