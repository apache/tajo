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

import socket
import google.protobuf.service
from controller import RpcController as Controller

class SocketFactory():
    @staticmethod
    def createSocket():
        return socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    @staticmethod
    def prepareHandle(host, port):
        s = SocketFactory.createSocket();
        try:
            s.connect((host, port))
        except socket.gaierror:
            s = None
        except socket.error:
            s = None

        return s

    @staticmethod
    def closeSocket(sock):
        if sock:
            try:
                sock.close()
            except:
                pass

        return


class RpcChannel(google.protobuf.service.RpcChannel):
    @staticmethod
    def createController():
        return Controller()

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.handle = SocketFactory.prepareHandle(self.host, self.port)

    def readMoreN(self, size):
        count = 0
        data = ""
        while True:
            try:
                tmpData = self.handle.recv(4096)
                count += len(tmpData)
                data += str(tmpData)
                if count >= size:
                    return data
            except:
                raise Exception("Socket Read Error")

    def closeSocket(self):
        SocketFactory.closeSocket(self.handle)
        self.handle = None

    def prepareRequest(self, method, request):
        return None

    def sendData(self, packets):
        try:
            for packet in packets:
                self.handle.sendall(packet)

        except socket.error:
            raise socket.error("packet send error")

    def sendRequest(self, rpc_request):
        return None

    def receiveResponse(self):
        return None

    def parseResponse(self, rpc_response, response_class):
        return None

    def CallMethod(self, method, controller, request, response_class, done):
        if self.handle is None:
            raise Exception("Connection Error(%s:%s)" % (self.host, self.port))

        rpc_request = self.prepareRequest(method, request)
        packets = self.sendRequest(rpc_request)
        self.sendData(packets)
        rpc_response = self.receiveResponse()
        response = self.parseResponse(rpc_response, response_class)
        return response
