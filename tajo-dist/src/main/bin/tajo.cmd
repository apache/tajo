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

@rem The Tajo command script
@rem
@rem Environment Variables
@rem
@rem   JAVA_HOME        The java implementation to use.  Overrides JAVA_HOME.
@rem
@rem   TAJO_CLASSPATH Extra Java CLASSPATH entries.
@rem
@rem   TAJO_USER_CLASSPATH_FIRST      When defined, the TAJO_CLASSPATH is
@rem                                  added in the beginning of the global
@rem                                  classpath. Can be defined, for example,
@rem                                  by doing
@rem                                  export TAJO_USER_CLASSPATH_FIRST=true
@rem
@rem   TAJO_MASTER_HEAPSIZE  The maximum amount of heap to use, in MB.
@rem                         Default is 1000.
@rem
@rem   TAJO_WORKER_HEAPSIZE  The maximum amount of heap to use, in MB.
@rem                         Default is 1000.
@rem
@rem   TAJO_OPTS      Extra Java runtime options.
@rem
@rem   TAJO_{COMMAND}_OPTS etc  TAJO_JT_OPTS applies to JobTracker
@rem                            for e.g.  TAJO_CLIENT_OPTS applies to
@rem                            more than one command (fs, dfs, fsck,
@rem                            dfsadmin etc)
@rem
@rem   TAJO_CONF_DIR  Alternate conf dir. Default is ${TAJO_HOME}/conf.
@rem

setlocal enabledelayedexpansion

set TAJO_HOME=%~dp0
for %%i in (%TAJO_HOME%.) do (
  set TAJO_HOME=%%~dpi
)
if "%TAJO_HOME:~-1%" == "\" (
  set TAJO_HOME=%TAJO_HOME:~0,-1%
)
set TAJO_BIN=%TAJO_HOME%\bin
cd %TAJO_BIN%

call %TAJO_BIN%\tajo-config.cmd %*

@rem get arguments
if "%1" == "--config" (
  shift
  shift
)

@rem if no args specified, show usage
if "%1" == "" (
  goto print_usage
  goto :eof
)

set tajo-command=%1
shift

@rem if command is help, show usage
set ifor=false
if "%tajo-command%" == "--help" set ifor=true
if "%tajo-command%" == "-help" set ifor=true 
if "%tajo-command%" == "-h" set ifor=true
if "%ifor%" == "true" (
  goto print_usage
  goto :eof
)

if exist %TAJO_CONF_DIR%\tajo-env.cmd (
  call %TAJO_CONF_DIR%\tajo-env.cmd
)

@rem some Java parameters
if not exist "%JAVA_HOME%\bin\java.exe" (
  echo Error: JAVA_HOME is incorrectly set.
  echo        Please update %TAJO_HOME%\conf\tajo-env.cmd
  goto :eof
)

set JAVA=%JAVA_HOME%\bin\java
set JAVA_TAJO_MASTER_HEAP_MAX=-Xmx1000m 
set JAVA_WORKER_HEAP_MAX=-Xmx1000m
set JAVA_QUERYMASTER_HEAP_MAX=-Xmx1000m
set JAVA_PULLSERVER_HEAP_MAX=-Xmx1000m

@rem check envvars which might override default args
if NOT "%TAJO_MASTER_HEAPSIZE%" == "" (
  set JAVA_TAJO_MASTER_HEAP_MAX=-Xmx%TAJO_MASTER_HEAPSIZE%m
)
if NOT "%TAJO_WORKER_HEAPSIZE%" == "" (
  set JAVA_WORKER_HEAP_MAX=-Xmx%TAJO_WORKER_HEAPSIZE%m
)
if NOT "%TAJO_PULLSERVER_HEAPSIZE%" == "" (
  set JAVA_PULLSERVER_HEAP_MAX="-Xmx""$TAJO_PULLSERVER_HEAPSIZE""m"

)
if NOT "%TAJO_QUERYMASTER_HEAPSIZE%" == "" (
  set JAVA_QUERYMASTER_HEAP_MAX=-Xmx%TAJO_QUERYMASTER_HEAPSIZE%m
)

@rem ##############################################################################
@rem Hadoop Checking Section Start
@rem ##############################################################################

if "%HADOOP_HOME%" == "" (
  echo Cannot find hadoop installation.
  echo Please update %TAJO_HOME%\conf\tajo-env.cmd
  goto :eof
)
set HADOOP=%HADOOP_HOME%\bin\hadoop
if not exist "%HADOOP%" (
  echo Cannot find hadoop installation.
  echo Please update %TAJO_HOME%\conf\tajo-env.cmd
  goto :eof
)

@rem Allow alternate conf dir location.
if not defined HADOOP_CONF_DIR (
  set HADOOP_CONF_DIR=%HADOOP_HOME%\etc\hadoop%
)

@rem CLASSPATH initially contains $HADOOP_CONF_DIR and tools.jar
set CLASSPATH=%HADOOP_CONF_DIR%;%JAVA_HOME%\lib\tools.jar

@rem ##############################################################################
@rem Hadoop Checking Section End
@rem ##############################################################################

@rem ##############################################################################
@rem Find and Set Hadoop CLASSPATH
@rem Hadoop Home Configuration Start
@rem ##############################################################################

@rem HADOOP JAR DIRS
set CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\share\hadoop\common\lib\*
set CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\share\hadoop\common\*
set CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\share\hadoop\hdfs\*
set CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\share\hadoop\hdfs\lib\*
set CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\share\hadoop\yarn\*
set CLASSPATH=%CLASSPATH%;%HADOOP_HOME%\share\hadoop\mapreduce\*

set HDFS_LIBRARY_PATH=%HADOOP_HOME%/bin/

@rem ##############################################################################
@rem Hadoop Home Configuration End
@rem ##############################################################################

@rem ##############################################################################
@rem Find and Set Tajo CLASSPATH
@rem Tajo Home Configuration Start
@rem ##############################################################################

@rem TAJO_BASE_CLASSPATH initially contains %TAJO_CONF_DIR%
set TAJO_BASE_CLASSPATH=%TAJO_HOME%\conf
set TAJO_BASE_CLASSPATH=%TAJO_BASE_CLASSPATH%;%TAJO_HOME%\*
set TAJO_BASE_CLASSPATH=%TAJO_BASE_CLASSPATH%;%TAJO_HOME%\lib\*
set TAJO_BASE_CLASSPATH=%TAJO_BASE_CLASSPATH%;%TAJO_HOME%\extlib\*

set CLASSPATH=%TAJO_BASE_CLASSPATH%;%CLASSPATH%

@rem ##############################################################################
@rem Tajo Home Configuration End
@rem ##############################################################################

@rem default log directory & file
if "%TAJO_LOG_DIR%" == "" (
  set TAJO_LOG_DIR=%TAJO_HOME%\logs
)
if "%TAJO_LOGFILE%" == "" (
  set TAJO_LOGFILE=tajo.log
)

@rem default policy file for service-level authorization
if "%TAJO_POLICYFILE%" == "" (
  set TAJO_POLICYFILE=tajo-policy.xml
)

@rem Disable IPv6 Support for network performance
set TAJO_OPTS=-Djava.net.preferIPv4Stack=true

@rem figure out which class to run
if "%tajo-command%"=="master" (
  set CLASS=org.apache.tajo.master.TajoMaster
  set TAJO_OPTS=%TAJO_OPTS% %JAVA_TAJO_MASTER_HEAP_MAX%
)
if "%tajo-command%"=="worker" (
  set CLASS=org.apache.tajo.worker.TajoWorker
  set TAJO_OPTS=%TAJO_OPTS% %JAVA_WORKER_HEAP_MAX%
)
if "%tajo-command%"=="pullserver" (
  set CLASS=org.apache.tajo.pullserver.TajoPullServer
  set TAJO_OPTS=%TAJO_OPTS% %JAVA_PULLSERVER_HEAP_MAX% %TAJO_PULLSERVER_OPTS%
)
if "%tajo-command%"=="catalog" (
  set CLASS=org.apache.tajo.catalog.CatalogServer
  set TAJO_OPTS=%TAJO_OPTS $TAJO_CATALOG_OPTS%
)
if "%tajo-command%"=="cli" (
  set CLASS=org.apache.tajo.cli.tsql.TajoCli
  if not defined TAJO_ROOT_LOGGER_APPENDER (
    set TAJO_ROOT_LOGGER_APPENDER=NullAppender
  )
  set TAJO_OPTS=%TAJO_OPTS% %TAJO_CLI_OPTS%
)
if "%tajo-command%"=="admin" (
  set CLASS=org.apache.tajo.cli.tools.TajoAdmin
  if not defined TAJO_ROOT_LOGGER_APPENDER (
    set TAJO_ROOT_LOGGER_APPENDER=NullAppender
  )
  set TAJO_OPTS=%TAJO_OPTS $TAJO_CLI_OPTS%
)
if "%tajo-command%"=="haadmin" (
  set CLASS=org.apache.tajo.cli.tools.TajoHAAdmin
  if not defined TAJO_ROOT_LOGGER_APPENDER (
    set TAJO_ROOT_LOGGER_APPENDER=NullAppender
  )
  set TAJO_OPTS=%TAJO_OPTS $TAJO_CLI_OPTS%
)
if "%tajo-command%"=="getconf" (
  set CLASS=org.apache.tajo.cli.tools.TajoGetConf
  if not defined TAJO_ROOT_LOGGER_APPENDER (
    set TAJO_ROOT_LOGGER_APPENDER=NullAppender
  )
  set TAJO_OPTS=%TAJO_OPTS $TAJO_CLI_OPTS%
)
if "%tajo-command%"=="dump" (
  set CLASS=org.apache.tajo.cli.tools.TajoDump
  if not defined TAJO_ROOT_LOGGER_APPENDER (
    set TAJO_ROOT_LOGGER_APPENDER=NullAppender
  )
  set TAJO_OPTS=%TAJO_OPTS $TAJO_DUMP_OPTS%
)
if "%tajo-command%"=="version" (
  set CLASS=%org.apache.tajo.util.VersionInfo%
  if not defined TAJO_ROOT_LOGGER_APPENDER (
    set TAJO_ROOT_LOGGER_APPENDER=NullAppender
  )
  set TAJO_OPTS=%TAJO_OPTS $TAJO_CLI_OPTS%
)

set TAJO_OPTS=%TAJO_OPTS% -Dtajo.log.dir=%TAJO_LOG_DIR%
set TAJO_OPTS=%TAJO_OPTS% -Dtajo.log.file=%TAJO_LOGFILE%
set TAJO_OPTS=%TAJO_OPTS% -Dtajo.home.dir=%TAJO_HOME%
set TAJO_OPTS=%TAJO_OPTS% -Dtajo.id.str=%TAJO_IDENT_STRING%
if not defined TAJO_ROOT_LOGGER_APPENDER (
  set TAJO_OPTS=%TAJO_OPTS% -Dtajo.root.logger.appender=console
) else (
  set TAJO_OPTS=%TAJO_OPTS% -Dtajo.root.logger.appender=%TAJO_ROOT_LOGGER_APPENDER%
)
if NOT "x%TAJO_ROOT_LOGGER_LEVEL%" == "x" (
  set TAJO_OPTS=%TAJO_OPTS% -Dtajo.root.logger.level=%TAJO_ROOT_LOGGER_LEVEL%
)
if NOT "x%JAVA_LIBRARY_PATH%" == "x" (
  set TAJO_OPTS=%TAJO_OPTS% -Djava.library.path=%JAVA_LIBRARY_PATH%
)

set TAJO_OPTS=%TAJO_OPTS% -Dtajo.policy.file=%TAJO_POLICYFILE%

@rem run it

echo starting %tajo-command%, logging to %TAJO_LOG_DIR%\%TAJO_LOGFILE%
call "%JAVA%" -Dproc_%tajo-command% %TAJO_OPTS% -Dtajo.id.str=%TAJO_IDENT_STRING% %CLASS%

:print_usage
  echo "Usage: tajo [--config confdir] COMMAND"
  echo "where COMMAND is one of:"
  echo "  master               run the Master Server"
  echo "  worker               run the Worker Server"
  echo "  pullserver           run the Pull Server"
  echo "  catalog              run the Catalog server"
  echo "  catutil              catalog utility"
  echo "  cli                  run the tajo cli"
  echo "  admin                run the tajo admin util"
  echo "  haadmin              run the tajo master HA admin util"
  echo "  getconf              print tajo configuration"
  echo "  jar <jar>            run a jar file"
  echo "  benchmark            run the benchmark driver"
  echo "  version              print the version"
  echo " or"
  echo "  CLASSNAME            run the class named CLASSNAME"
  echo "Most commands print help when invoked w/o parameters."

endlocal
