@echo off
@rem Licensed to the Apache Software Foundation (ASF) under one or more
@rem contributor license agreements.  See the NOTICE file distributed with
@rem this work for additional information regarding copyright ownership.
@rem The ASF licenses this file to You under the Apache License, Version 2.0
@rem (the "License"); you may not use this file except in compliance with
@rem the License.  You may obtain a copy of the License at
@rem
@rem     http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.

@rem Set Tajo-specific environment variables here.
@rem The only required environ

@rem Hadoop home. Required
set HADOOP_HOME=

@rem The java implementation to use.  Required.
set JAVA_HOME=%JAVA_HOME%

@rem Extra Java CLASSPATH elements.  Optional.
@rem set TAJO_CLASSPATH=/xxx/extlib/*:/xxx/xxx.jar

@rem The maximum amount of heap to use, in MB. Default is 1000.
@rem set TAJO_MASTER_HEAPSIZE=1000

@rem The maximum amount of heap to use, in MB. Default is 1000.
@rem set TAJO_WORKER_HEAPSIZE=1000

@rem The maximum amount of heap to use, in MB. Default is 1000.
@rem set TAJO_PULLSERVER_HEAPSIZE=1000

@rem The maximum amount of heap to use, in MB. Default is 1000.
@rem set TAJO_QUERYMASTER_HEAPSIZE=1000

@rem Extra Java runtime options.  Empty by default.
@rem set TAJO_OPTS=-server

@rem Extra TajoMaster's java runtime options for TajoMaster. Empty by default
@rem set TAJO_MASTER_OPTS=

@rem Extra TajoWorker's java runtime options. Empty by default
@rem set TAJO_WORKER_OPTS=

@rem Extra TajoPullServer's java runtime options. Empty by default
@rem set TAJO_PULLSERVER_OPTS=

@rem Extra  QueryMaster mode TajoWorker's java runtime options for TajoMaster. Empty by default
@rem set TAJO_QUERYMASTER_OPTS=

@rem Where log files are stored.  %TAJO_HOME%\logs by default.
@rem set TAJO_LOG_DIR=%TAJO_HOME%\logs

@rem The directory where pid files are stored. /tmp by default.
@rem set TAJO_PID_DIR=%TAJO_HOME%\pids

@rem A string representing this instance of tajo. %USERNAME% by default.
@rem set TAJO_IDENT_STRING=%USERNAME%

@rem The scheduling priority for daemon processes.  See 'man nice'.
@rem set TAJO_NICENESS=10

@rem Tajo cluster mode. the default mode is standby mode.
set TAJO_WORKER_STANDBY_MODE=true

@rem It must be required to use HCatalogStore
@rem set HIVE_HOME=
@rem set HIVE_JDBC_DRIVER_DIR=

@rem Tajo PullServer mode. the default mode is standalone mode
@rem set TAJO_PULLSERVER_STANDALONE=false
