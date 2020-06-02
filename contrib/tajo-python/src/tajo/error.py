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

class TajoClientError(Exception):
    """The base class for other Tajo Client exceptions"""

    def __init__(self, value):
        super(TajoClientError, self).__init__(value)
        self.value = value

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return ("<TajoClientError in " + repr(self.value) + ">")

class NotImplementedMethodError(TajoClientError):
    """The NotImplementedMethodError exceptions"""

    def __init__(self, value):
        super(NotImplementedMethodError, self).__init__(value)
        self.value = value

    def __repr__(self):
        return ("<NotImplementedMethodError in " + repr(self.value) + ">")

class InvalidStatusError(TajoClientError):
    """Invalid Status Error exceptions"""

    def __init__(self, value):
        super(InvalidStatusError, self).__init__(value)
        self.value = value

    def __repr__(self):
        return ("<InvalidStatusError in " + repr(self.value) + ">")

class InvalidRequestError(TajoClientError):
    """Invalid Request Error exceptions"""

    def __init__(self, value):
        super(InvalidRequestError, self).__init__(value)
        self.value = value

    def __repr__(self):
        return ("<InvalidResquestError in " + repr(self.value) + "is not valid>")

class NotFoundError(TajoClientError):
    """NotFound Error exceptions"""

    def __init__(self, value):
        super(NotFoundError, self).__init__(value)
        self.value = value

    def __repr__(self):
        return ("<NotFoundError in " + repr(self.value) + ">")

class NullQueryIdError(TajoClientError):
    """NotFound Error exceptions"""

    def __init__(self):
        super(NullQueryIdError, self).__init__()

    def __repr__(self):
        return ("<NullQueryIdError>")

class InternalError(TajoClientError):
    """NotFound Error exceptions"""

    def __init__(self, value):
        super(InternalError, self).__init__(value)
        self.value = value

    def __repr__(self):
        return ("<InternalError in " + repr(self.value) + ">")
