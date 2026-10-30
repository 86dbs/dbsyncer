@echo off
setlocal

for %%F in ("%~dp0..") do set "DBS_HOME=%%~fF\"
echo DBS_HOME=%DBS_HOME%
cd /d "%DBS_HOME%"

rem Resolve Java: embedded jre8 > JRE_HOME > JAVA_HOME > PATH
if not defined JRE_HOME if exist "%DBS_HOME%jre8\bin\java.exe" set "JRE_HOME=%DBS_HOME%jre8"
if defined JRE_HOME (
  echo Using JRE_HOME: %JRE_HOME%
  set "JAVA_ROOT=%JRE_HOME%"
) else if defined JAVA_HOME (
  echo Using JAVA_HOME: %JAVA_HOME%
  set "JAVA_ROOT=%JAVA_HOME%"
) else (
  set "JAVA_ROOT="
)

set "JAVA_BIN=java"
if defined JAVA_ROOT if exist "%JAVA_ROOT%\bin\java.exe" set "JAVA_BIN=%JAVA_ROOT%\bin\java.exe"
if defined JAVA_ROOT if exist "%JAVA_ROOT%\jre\bin\java.exe" if not exist "%JAVA_ROOT%\bin\java.exe" set "JAVA_BIN=%JAVA_ROOT%\jre\bin\java.exe"

set "JAVA_EXT_DIR="
if defined JAVA_ROOT if exist "%JAVA_ROOT%\lib\ext" set "JAVA_EXT_DIR=%JAVA_ROOT%\lib\ext"
if defined JAVA_ROOT if exist "%JAVA_ROOT%\jre\lib\ext" if not defined JAVA_EXT_DIR set "JAVA_EXT_DIR=%JAVA_ROOT%\jre\lib\ext"

if not defined JAVA_EXT_DIR (
  echo ERROR: Cannot find JRE lib\ext. Set JAVA_HOME to JDK/JRE 8.
  exit /b 1
)
echo JAVA_BIN=%JAVA_BIN%
echo JAVA_EXT_DIR=%JAVA_EXT_DIR%

set "SERVER_OPTS=-Xms3800m -Xmx3800m -Xmn1500m -Xss512k -XX:MetaspaceSize=192m -XX:+DisableAttachMechanism"
set "ENCRYPT_FILE=%DBS_HOME%bin\libDBSyncer.dll"
if exist "%ENCRYPT_FILE%" set "SERVER_OPTS=%SERVER_OPTS% -agentpath:%ENCRYPT_FILE%"

rem Do not nest quotes inside -Djava.ext.dirs, or cmd will split the line
set "SERVER_OPTS=%SERVER_OPTS% -Djava.ext.dirs=%JAVA_EXT_DIR%;%DBS_HOME%lib"
set "SERVER_OPTS=%SERVER_OPTS% -Dspring.config.location=%DBS_HOME%conf\application.properties"
set "SERVER_OPTS=%SERVER_OPTS% -DLOG_PATH=%DBS_HOME%logs"
set "SERVER_OPTS=%SERVER_OPTS% -Dsun.stdout.encoding=UTF-8 -Dfile.encoding=UTF-8 -Duser.timezone=Asia/Shanghai -Duser.dir=%DBS_HOME%"
set "SERVER_OPTS=%SERVER_OPTS% -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:ParallelGCThreads=4 -XX:+CMSClassUnloadingEnabled -XX:+DisableExplicitGC"
set "SERVER_OPTS=%SERVER_OPTS% -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=68"

echo Starting DBSyncer...
"%JAVA_BIN%" %SERVER_OPTS% org.dbsyncer.web.Application
endlocal
