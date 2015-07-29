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

class ResultSetBase(object):
    def __init__(self):
        self.cur_row = 0
        self.cur = None
        self.schema = None
        self.total_row = 0

    def current_tuple(self):
        return self.cur

    def next(self):
        if self.total_row <= 0:
            return False

        self.cur = self.next_tuple()
        self.cur_row += 1
        if self.cur is not None:
            return True

        return False

    def next_tuple(self):
        raise Exception("Not Implemented")


