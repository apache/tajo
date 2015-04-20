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
from tajo_udaf import AbstractUdaf


class SumPy(AbstractUdaf):
    name = 'sum_py'
    aggregated = 0

    def __init__(self, aggregated=0):
        self.aggregated = aggregated

    # return the function name
    @output_type('text')
    def name(self):
        return self.name

    # eval at the first stage
    @output_type('int8')
    def eval(self, item):
        self.aggregated += item

    # merge the result of the first stage
    @output_type('int8')
    def merge(self, item):
        self.aggregated += item

    # get the final result
    @output_type('int8')
    def terminate(self):
        return self.aggregated
