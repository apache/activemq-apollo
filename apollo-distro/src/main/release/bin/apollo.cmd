@REM
@REM Licensed to the Apache Software Foundation (ASF) under one or more
@REM contributor license agreements.  See the NOTICE file distributed with
@REM this work for additional information regarding copyright ownership.
@REM The ASF licenses this file to You under the Apache License, Version 2.0
@REM (the "License"); you may not use this file except in compliance with
@REM the License.  You may obtain a copy of the License at
@REM
@REM      http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM See the License for the specific language governing permissions and
@REM limitations under the License.
@REM
@echo off

setlocal

if NOT "%APOLLO_HOME%"=="" goto CHECK_APOLLO_HOME
PUSHD .
CD %~dp0..
set APOLLO_HOME=%CD%
POPD

:CHECK_APOLLO_HOME
if exist "%APOLLO_HOME%\bin\apollo.cmd" goto CHECK_JAVA

:NO_HOME
echo APOLLO_HOME environment variable is set incorrectly. Please set APOLLO_HOME.
goto END

:CHECK_JAVA
set _JAVACMD=%JAVACMD%

if "%JAVA_HOME%" == "" goto NO_JAVA_HOME
if not exist "%JAVA_HOME%\bin\java.exe" goto NO_JAVA_HOME
if "%_JAVACMD%" == "" set _JAVACMD=%JAVA_HOME%\bin\java.exe
goto RUN_JAVA

:NO_JAVA_HOME
if "%_JAVACMD%" == "" set _JAVACMD=java.exe
echo.
echo Warning: JAVA_HOME environment variable is not set.
echo.

:RUN_JAVA

set CLASSPATH=%APOLLO_HOME%\lib\apollo-boot.jar

set BOOTDIRS=%APOLLO_HOME%\lib
if NOT "x%APOLLO_BASE%" == "x" set BOOTDIRS=%APOLLO_BASE%\lib;%BOOTDIRS%

if "%JVM_FLAGS%" == "" set JVM_FLAGS=-server -Xmx1G -XX:-UseBiasedLocking

if "%APOLLO_ASSERTIONS%"=="false" goto noAPOLLO_ASSERTIONS
  set JVM_FLAGS=-ea %JVM_FLAGS%
:noAPOLLO_ASSERTIONS

if "x%APOLLO_OPTS%" == "x" goto noAPOLLO_OPTS
  set JVM_FLAGS=%JVM_FLAGS% %APOLLO_OPTS%
:noAPOLLO_OPTS

if "x%APOLLO_DEBUG%" == "x" goto noDEBUG
  set JVM_FLAGS=%JVM_FLAGS% -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005
:noDEBUG

if "x%APOLLO_PROFILE%" == "x" goto noPROFILE
  set JVM_FLAGS=-agentlib:yjpagent %JVM_FLAGS%
:noPROFILE

if "%JMX_OPTS%" == "" set JMX_OPTS=-Dcom.sun.management.jmxremote
rem set JMX_OPTS=-Dcom.sun.management.jmxremote.port=1099 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false
set JVM_FLAGS=%JVM_FLAGS% %JMX_OPTS%

set JVM_FLAGS=%JVM_FLAGS% -Dapollo.home="%APOLLO_HOME%"
if NOT "x%APOLLO_BASE%" == "x" set JVM_FLAGS=%JVM_FLAGS% -Dapollo.base="%APOLLO_BASE%"
set JVM_FLAGS=%JVM_FLAGS% -classpath "%CLASSPATH%"

"%_JAVACMD%" %JVM_FLAGS% org.apache.activemq.apollo.boot.Apollo "%BOOTDIRS%" org.apache.activemq.apollo.cli.Apollo %*

:END
endlocal
GOTO :EOF

:EOF
