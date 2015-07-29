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
from tajo.rowdecoder import RowDecoder
from tajo.resultsetbase import ResultSetBase
from tajo.py3 import httplib
import struct

try:
    import simplejson as json
except ImportError:
    import json

class TajoMemoryResultSet(ResultSetBase):
    def __init__(self, resultset_info, eos, start_offset, count, content):
        super(TajoMemoryResultSet, self).__init__()
        self.schema = None
        self.decoder = None
        self.total_size = 0

        if resultset_info is not None:
            self.schema = resultset_info.schema()
            self.decoder = RowDecoder(self.schema)

        if content is not None:
            self.total_size = len(content)

        self.total_row = count
        self.start_offset = start_offset
        self.data = content
        self.eos = eos

        self.cur = None
        self.data_offset = 0

    def next_tuple(self):
        if self.data_offset < self.total_size:
            dsize = struct.unpack('>I', self.data[self.data_offset:self.data_offset+4])[0]
            self.data_offset += 4
            tuple_data = self.data[self.data_offset:self.data_offset+dsize]
            self.data_offset += dsize
            self.cur = self.decoder.toTuple(tuple_data)
            return self.cur

        return None

    @staticmethod
    def create(resultset_info, headers, content):
        offset = 0
        count = 0
        eos = False

        if "x-tajo-offset" in headers:
            offset = int(headers["x-tajo-offset"])

        if "x-tajo-count" in headers:
            count = int(headers["x-tajo-count"])

        if "x-tajo-eos" in headers:
            if headers["x-tajo-eos"] == "true":
                eos = True

        if offset < 0:
            offset = 0

        return TajoMemoryResultSet(resultset_info, eos, offset, count, content)


class TajoMemoryResultSetRequest(TajoRequest):
    object_cls = TajoMemoryResultSet
    ok_status = [httplib.OK]

    def __init__(self, info, base, count):
        self.info = info
        self.url = str(info)
        self.base = base
        self.count = count

    def uri(self):
        body = self.url[len(self.base):]
        url = "%s?count=%s"%(body, self.count)

        return url

    def headers(self):
        return {'Content-Type': 'application/octet-stream'}

    def params(self):
        return None

    def cls(self):
        return self.object_cls

    def request(self, conn):
        headers, contents = conn._request(self.method(), self.uri(), self.params())
        self.check_status(headers, contents)
        return self.object_cls.create(self.info, headers, contents)
