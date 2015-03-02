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

from rpc.service import RpcService
from channel import TajoRpcChannel

import proto.TajoMasterClientProtocol_pb2 as tajo_pb2
import proto.ClientProtos_pb2 as ClientProtos_pb2

class TajoSessionConnection():
    def __init__(self, host, port, username="tmpuser"):
        self.host = host
        self.port = port
        self.username = username
        self.sessionId = None
        self.channel = TajoRpcChannel(self.host, self.port)
        self.service = RpcService(tajo_pb2.TajoMasterClientProtocolService_Stub,
                                  self.channel)

    def createNewSessionId(self):
        request = ClientProtos_pb2.CreateSessionRequest()
        request.username = self.username
        response = self.service.createSession(request)
        self.sessionId = response.sessionId.id
        return self.sessionId

    def checkSessionAndGet(self):
        if self.sessionId is None:
            self.sessionId = self.createNewSessionId()

        return self.sessionId

