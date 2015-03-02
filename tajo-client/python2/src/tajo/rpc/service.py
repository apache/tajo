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

class RpcService(object):
    def __init__(self, service_stub_cls, channel):
        self.service_stub_cls = service_stub_cls
        self.channel = channel
        self.service = self.service_stub_cls(self.channel)

        for method in self.service_stub_cls.GetDescriptor().methods:
            func = lambda request, service=self, method=method.name, callback=None: \
                service.call(service_stub_cls.__dict__[method], request, callback)

            self.__dict__[method.name] = func

    def call(self, func, request, callback):
        controller = self.channel.createController()
        return func(self.service, controller, request, callback)
