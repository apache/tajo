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


class AbstractUdaf:

    def __init__(self):
        return

    @output_type('text')
    def name(self):
        """Return the function name"""
        return

    def eval(self, item):
        """Eval item at the first stage"""
        return

    def merge(self, item):
        """Merge the result of the first stage"""
        return

    def terminate(self):
        """Get the final result"""
        return
