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

import google.protobuf.service

class RpcController(google.protobuf.service.RpcController):
    def __init__(self):
        self.error_code = None
        self.error_message = None

    def handleError(self, error_code, error_message):
        self.error_code = error_code
        self.error_message = error_message

    def reset(self):
        self.error_code = None
        self.error_message = None

    def failed(self):
        return self.error_message is not None

    def error(self):
        return self.error_message
