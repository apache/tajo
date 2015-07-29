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

from tajo.error import InvalidStatusError
from tajo.base import TajoRequest, TajoObject, TajoPostRequest, TajoDeleteRequest
from tajo.py3 import httplib, PY3

try:
    import simplejson as json
except ImportError:
    import json

class TajoCreateDatabase(TajoObject):
    @staticmethod
    def create(headers, content):
        return True;


class TajoDeleteDatabase(TajoObject):
    @staticmethod
    def create(headers, content):
        return True;


class TajoDatabaseName(TajoObject):
    def __init__(self, name):
        self.database_name = name

    def __repr__(self):
        return self.database_name


class TajoDatabase(TajoObject):
    def __init__(self, objs):
        self.objs = objs

    def __repr__(self):
        return self.objs["name"]

    @staticmethod
    def create(headers, content):
        if PY3:
            content = content.decode('utf-8')

        data = json.loads(content)
        return TajoDatabase(data)


class TajoDatabases(TajoObject):
    @staticmethod
    def create(headers, content):
        if PY3:
            content = content.decode('utf-8')

        data = json.loads(content)
        databases = []
        for database in data["databases"]:
            databases.append(TajoDatabaseName(database))

        return databases


class TajoDatabaseRequest(TajoRequest):
    object_cls = TajoDatabase
    ok_status = [httplib.OK]

    def __init__(self, database):
        self.database_name = database.database_name

    def uri(self):
        return "databases/%s"%(self.database_name)

    def headers(self):
        return None

    def params(self):
        return None

    def cls(self):
        return self.object_cls


class TajoDatabasesRequest(TajoRequest):
    object_cls = TajoDatabases
    ok_status = [httplib.OK]

    def __init__(self):
        pass

    def uri(self):
        return "databases"

    def headers(self):
        return None

    def params(self):
        return None

    def cls(self):
        return self.object_cls


class TajoCreateDatabaseRequest(TajoPostRequest):
    object_cls = TajoCreateDatabase
    ok_status = [httplib.CREATED]

    def __init__(self, database_name):
        self.database_name = database_name

    def uri(self):
        return "databases"

    def headers(self):
        return None

    def params(self):
        payload = {
            "databaseName": self.database_name
        }
        return payload

    def cls(self):
        return self.object_cls


class TajoDeleteDatabaseRequest(TajoDeleteRequest):
    object_cls = TajoDeleteDatabase
    ok_status = [httplib.OK]

    def __init__(self, database_name):
        self.database_name = database_name

    def uri(self):
        return "databases/%s"%(self.database_name)

    def headers(self):
        return None

    def params(self):
        return None

    def cls(self):
        return self.object_cls
