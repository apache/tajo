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

import google.protobuf.message

class ProtoUtils():
    @staticmethod
    def VarSize(uint64):
        if uint64 <= 0x7f: return 1
        if uint64 <= 0x3fff: return 2
        if uint64 <= 0x1fffff: return 3
        if uint64 <= 0xfffffff: return 4
        return 5

    @staticmethod
    def EncodeVarint(value):
        v = []
        bits = value & 0x7f
        value >>= 7
        while value:
            v.append(chr(0x80|bits))
            bits = value & 0x7f
            value >>= 7

        v.append(chr(bits))
        return ''.join(v)

    @staticmethod
    def DecodeVarint(buffer):
        pos = 0
        result = 0
        shift = 0
        mask = (1 << 64) - 1
        while 1:
            b = ord(buffer[pos])
            result |= ((b & 0x7f) << shift)
            pos += 1

            if not (b & 0x80):
                result &= mask
                return result, pos

            shift += 7
            if shift >= 64:
                raise google.protobuf.message.DecodeError('Too many bytes when decoding varint.')
