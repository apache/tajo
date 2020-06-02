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

import httplib2 as Http
import json

class TajoConnection(object):
    def __init__(self, base):
        self.base = base
        self.request = Http.Http()
        self.common_headers = {'Content-Type': 'application/json'}

    def add_header(self, key, value):
        self.common_headers[key] = value

    def post(self, uri, body, headers = None):
        return self._request("POST", uri, body, headers)

    def get(self, uri, body, headers = None):
        return self._request("GET", uri, body, headers)

    def _request(self, method, uri, body = None, headers = None):
        url = "%s/%s"%(self.base, uri)
        aheaders = self.common_headers.copy()
        if headers is not None:
            aheaders.update(headers)

        return self.request.request(url, method, body=json.dumps(body), headers=aheaders)
