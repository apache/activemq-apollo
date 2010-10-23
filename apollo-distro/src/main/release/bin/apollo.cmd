@echo off
rem ------------------------------------------------------------------------
rem
rem  Licensed to the Apache Software Foundation (ASF) under one or more
rem  contributor license agreements.  See the NOTICE file distributed with
rem  this work for additional information regarding copyright ownership.
rem  The ASF licenses this file to You under the Apache License, Version 2.0
rem  (the "License"); you may not use this file except in compliance with
rem  the License.  You may obtain a copy of the License at
rem  
rem  http://www.apache.org/licenses/LICENSE-2.0
rem  
rem  Unless required by applicable law or agreed to in writing, software
rem  distributed under the License is distributed on an "AS IS" BASIS,
rem  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem  See the License for the specific language governing permissions and
rem  limitations under the License.
rem
rem ------------------------------------------------------------------------

setlocal

if "%APOLLO_HOME%"=="" set APOLLO_HOME=%~dp0..
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

if "%APOLLO_BASE%" == "" set APOLLO_BASE=%APOLLO_HOME%

set CLASSPATH=%APOLLO_BASE%\etc

rem if not exist "%APOLLO_HOME%\lib\patches" goto NO_LIB_PATCHES
rem for %%i in ("%APOLLO_HOME%\lib\patches\*.jar") do call :ADD_CLASSPATH %%i
rem :NO_LIB_PATCHES

for %%i in ("%APOLLO_HOME%\lib\*.jar") do call :ADD_CLASSPATH %%i

if "%APOLLO_OPTS%" == "" set APOLLO_OPTS=-server -Xmx1G

set JVM_FLAGS=%APOLLO_OPTS%

if "x%APOLLO_DEBUG%" == "x" goto noDEBUG
  set JVM_FLAGS=%JVM_FLAGS% -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspEND=n,address=5005
:noDEBUG

if "x%APOLLO_PROFILE%" == "x" goto noPROFILE
  set JVM_FLAGS=-agentlib:yjpagent %JVM_FLAGS%
:noPROFILE

if "%JMX_OPTS%" == "" set JMX_OPTS=-Dcom.sun.management.jmxremote
rem set JMX_OPTS=-Dcom.sun.management.jmxremote.port=1099 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false
set JVM_FLAGS=%JVM_FLAGS% %JMX_OPTS%

set JVM_FLAGS=%JVM_FLAGS% -Dapollo.home="%APOLLO_HOME%" -Dapollo.base="%APOLLO_BASE%"
set JVM_FLAGS=%JVM_FLAGS% -classpath "%CLASSPATH%"

rem echo "%_JAVACMD%" %JVM_FLAGS%  org.apache.activemq.apollo.cli.Apollo %*
"%_JAVACMD%" %JVM_FLAGS%  org.apache.activemq.apollo.cli.Apollo %*

:END
endlocal
GOTO :EOF

:ADD_CLASSPATH
 set CLASSPATH=%CLASSPATH%;%1
 GOTO :EOF

:EOF