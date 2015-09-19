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

@rem Runs a Tajo command as a daemon.
@rem
@rem Environment Variables
@rem
@rem   TAJO_CONF_DIR  Alternate conf dir. Default is %TAJO_HOME%\conf.
@rem   TAJO_LOG_DIR   Where log files are stored.  PWD by default.
@rem   TAJO_MASTER    host:path where tajo code should be rsync'd from
@rem   TAJO_IDENT_STRING   A string representing this instance of tajo. %USERNAME% by default
@rem   TAJO_NICENESS The scheduling priority for daemons. Defaults to 0.

setlocal enabledelayedexpansion

if "%2" == "" (
  echo "Usage: tajo-daemon.cmd [--config <conf-dir>] (start|stop) <tajo-command> <args...>"
  goto :eof
)

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
set startStop=%1
shift
set tajo-command=%1
shift

if not defined TAJO_IDENT_STRING (
  set TAJO_IDENT_STRING=%USERNAME%
)

if not defined TAJO_LOG_DIR (
  set TAJO_LOG_DIR=%TAJO_HOME%\logs
)
if not exist %TAJO_LOG_DIR% (
  mkdir %TAJO_LOG_DIR%
)

@rem some variables
set TAJO_LOGFILE=tajo-%TAJO_IDENT_STRING%-%tajo-command%-%USERDOMAIN%.log
if not defined TAJO_ROOT_LOGGER_APPENDER (
  set TAJO_ROOT_LOGGER_APPENDER=DRFA
)
if not defined TAJO_PULLSERVER_STANDALONE (
 set TAJO_PULLSERVER_STANDALONE=false
)
set log=%TAJO_LOG_DIR%\tajo-%TAJO_IDENT_STRING%-%tajo-command%-%USERDOMAIN%.out

@rem excute command
if "%startStop%" == "start" (
  goto startProcess
)
if "%startStop%" == "stop" (
  goto stopProcess
)

:startProcess
  Tasklist /FI "WINDOWTITLE eq Apache Tajo - tajo   %tajo-command%" 2>NUL | find /I /N "cmd.exe">NUL
  if %ERRORLEVEL%==0 (
    echo %tajo-command% running. Stop it first.
  ) else (
    start "Apache Tajo" tajo %tajo-command%
  )
  goto :eof

:stopProcess
  Taskkill /FI "WINDOWTITLE eq Apache Tajo - tajo   %tajo-command%"
  goto :eof

endlocal
