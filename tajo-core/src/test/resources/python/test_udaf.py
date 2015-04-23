############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from tajo_util import output_type


class AvgPy:
    sum = 0
    cnt = 0

    def __init__(self):
        self.sum = 0
        self.cnt = 0

    # eval at the first stage
    def eval(self, item):
        self.sum += item
        self.cnt += 1

    # get intermediate result
    @output_type('sum:int4', 'cnt:int4')
    def get_partial_result(self):
        return [self.sum, self.cnt]

    # merge intermediate results
    def merge(self, sum, cnt):
        self.sum += sum
        self.cnt += cnt

    # get final result
    @output_type('float4')
    def get_final_result(self):
        return self.sum / self.cnt


class CountPy:
    cnt = 0

    def __init__(self):
        self.cnt = 0

    # eval at the first stage
    def eval(self):
        self.cnt += 1

    # get intermediate result
    @output_type('cnt:int4')
    def get_partial_result(self):
        return [self.cnt]

    # merge intermediate results
    def merge(self, cnt):
        self.cnt += cnt

    # get final result
    @output_type('float4')
    def get_final_result(self):
        return self.cnt