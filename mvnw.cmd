@REM ----------------------------------------------------------------------------
@REM Licensed to the Apache Software Foundation (ASF) under one
@REM or more contributor license agreements.  See the NOTICE file
@REM distributed with this work for additional information
@REM regarding copyright ownership.  The ASF licenses this file
@REM to you under the Apache License, Version 2.0 (the
@REM "License"); you may not use this file except in compliance
@REM with the License.  You may obtain a copy of the License at
@REM
@REM    https://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing,
@REM software distributed under the License is distributed on an
@REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM KIND, either express or implied.  See the License for the
@REM specific language governing permissions and limitations
@REM under the License.
@REM ----------------------------------------------------------------------------

@REM Maven Wrapper start script for Windows

@REM Find the project base directory
@SETLOCAL ENABLEEXTENSIONS
@SET APP_HOME=%~dp0
@SET APP_HOME=%APP_HOME:~0,-1%

@SET DEFAULT_JVM_OPTS="-Xmx64m" "-Xms64m"

@IF NOT DEFINED JAVA_HOME GOTO :findJavaFromPath
@SET JAVA_EXEC=%JAVA_HOME%\bin\java.exe
@IF EXIST "%JAVA_EXEC%" GOTO :execute
:findJavaFromPath
@FOR %%i IN (java.exe) DO @SET JAVA_EXEC=%%~$PATH:i
@IF DEFINED JAVA_EXEC GOTO :execute
@ECHO ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
@ECHO Please set the JAVA_HOME variable in your environment to match the
@ECHO location of your Java installation.
@EXIT /B 1
:execute

@SET MAVEN_PROJECTBASEDIR=%MAVEN_BASEDIR%
@IF NOT DEFINED MAVEN_PROJECTBASEDIR SET MAVEN_PROJECTBASEDIR=%CD%

@SET WRAPPER_LAUNCHER=org.apache.maven.wrapper.MavenWrapperMain

@"%JAVA_EXEC%" %DEFAULT_JVM_OPTS% %JAVA_OPTS% -classpath "%APP_HOME%\.mvn\wrapper\maven-wrapper.jar" %WRAPPER_LAUNCHER% %*
@ENDLOCAL
