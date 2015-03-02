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

import io
import struct
import math
from datatypes import TajoDataTypes as ttype

class RowDecoder:
    def __init__(self, schema):
        self.schema = schema
        self.headerSize = int(math.ceil(float(len(self.schema.fields)) / 8))

    def toTuples(self, serializedTuples):
        results = []
        for serializedTuple in serializedTuples:
            results.append(self.toTuple(serializedTuple))

        return tuple(results)

    def toTuple(self, serializedTuple):
        size = len(self.schema.fields)
        nullFlags = serializedTuple[:self.headerSize]
        bb = io.BytesIO(serializedTuple[self.headerSize:])
        results = []

        for i in range(size):
            field = self.schema.fields[i]
            fieldType = field.dataType.type
            results.append(self.convert(0, fieldType, bb))

        return tuple(results)

    def convert(self, isNull, ftype, bb):
        if (isNull == 1):
            return "NULL"

        if ftype == ttype.INT1:
            v = bb.read(1)
            return struct.unpack("b", v)[0]

        if ftype == ttype.INT2:
            v = bb.read(2)
            return struct.unpack(">h", v)[0]

        if ftype == ttype.INT4 or ftype == ttype.DATE:
            v = bb.read(4)
            return struct.unpack(">i", v)[0]

        if ftype == ttype.INT8 or ftype == ttype.TIME or ftype == ttype.TIMESTAMP:
            v = bb.read(8)
            return struct.unpack(">q", v)[0]

        if ftype == ttype.FLOAT4:
            v = bb.read(4)
            return struct.unpack(">f", v)[0]

        if ftype == ttype.FLOAT8:
            v = bb.read(8)
            return struct.unpack(">d", v)[0]

        if ftype == ttype.TEXT or ftype == ttype.BLOB:
            l = bb.read(4)
            l2 = struct.unpack(">i", l)[0]
            v = bb.read(l2)
            return v
