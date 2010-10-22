@REM
@REM  Licensed to the Apache Software Foundation (ASF) under one or more
@REM  contributor license agreements.  See the NOTICE file distributed with
@REM  this work for additional information regarding copyright ownership.
@REM  The ASF licenses this file to You under the Apache License, Version 2.0
@REM  (the "License"); you may not use this file except in compliance with
@REM  the License.  You may obtain a copy of the License at
@REM  
@REM  http://www.apache.org/licenses/LICENSE-2.0
@REM  
@REM  Unless required by applicable law or agreed to in writing, software
@REM  distributed under the License is distributed on an "AS IS" BASIS,
@REM  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM  See the License for the specific language governing permissions and
@REM  limitations under the License.
@REM
@echo off

REM ------------------------------------------------------------------------
if "%OS%"=="Windows_NT" @setlocal

rem %~dp0 is expanded pathname of the current script under NT
set DEFAULT_APOLLO_HOME=%~dp0..

if "%APOLLO_HOME%"=="" set APOLLO_HOME=%DEFAULT_APOLLO_HOME%
set DEFAULT_APOLLO_HOME=

:doneStart
rem find APOLLO_HOME if it does not exist due to either an invalid value passed
rem by the user or the %0 problem on Windows 9x
if exist "%APOLLO_HOME%\readme.html" goto checkJava

rem check for apollo in Program Files on system drive
if not exist "%SystemDrive%\Program Files\apollo" goto checkSystemDrive
set APOLLO_HOME=%SystemDrive%\Program Files\apollo
goto checkJava

:checkSystemDrive
rem check for apollo in root directory of system drive
if not exist %SystemDrive%\apollo\readme.html goto checkCDrive
set APOLLO_HOME=%SystemDrive%\apollo
goto checkJava

:checkCDrive
rem check for apollo in C:\apollo for Win9X users
if not exist C:\apollo\readme.html goto noHome
set APOLLO_HOME=C:\apollo
goto checkJava

:noHome
echo APOLLO_HOME is set incorrectly or apollo could not be located. Please set APOLLO_HOME.
goto end

:checkJava
set _JAVACMD=%JAVACMD%

if "%JAVA_HOME%" == "" goto noJavaHome
if not exist "%JAVA_HOME%\bin\java.exe" goto noJavaHome
if "%_JAVACMD%" == "" set _JAVACMD=%JAVA_HOME%\bin\java.exe
goto runJava

:noJavaHome
if "%_JAVACMD%" == "" set _JAVACMD=java.exe
echo.
echo Warning: JAVA_HOME environment variable is not set.
echo.

:runJava

SET CLASSPATH=%APOLLO_HOME%\lib\*;%APOLLO_HOME%\lib\tool\*;%APOLLO_HOME%\lib\optional\*

if "%APOLLO_BASE%" == "" set APOLLO_BASE=%APOLLO_HOME%

if "%APOLLO_OPTS%" == "" set APOLLO_OPTS=-server -Xmx1G

set JVM_FLAGS=%APOLLO_OPTS%

if "x%APOLLO_DEBUG%" == "x" goto noDEBUG
  set JVM_FLAGS=%JVM_FLAGS% -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005
:noDEBUG

if "x%APOLLO_PROFILE%" == "x" goto noPROFILE
  set JVM_FLAGS=-agentlib:yjpagent %JVM_FLAGS%
:noPROFILE

if "%JMX_OPTS%" == "" set JMX_OPTS=-Dcom.sun.management.jmxremote
REM set JMX_OPTS=-Dcom.sun.management.jmxremote.port=1099 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false
set JVM_FLAGS=%JVM_FLAGS% %JMX_OPTS%

set JVM_FLAGS=%JVM_FLAGS% -Dapollo.home="%APOLLO_HOME%" -Dapollo.base="%APOLLO_BASE%"
set JVM_FLAGS=%JVM_FLAGS% -classpath "%CLASSPATH%"

"%_JAVACMD%" %JVM_FLAGS%  org.apache.activemq.apollo.cli.Apollo %*

:end
set _JAVACMD=
if "%OS%"=="Windows_NT" @endlocal

