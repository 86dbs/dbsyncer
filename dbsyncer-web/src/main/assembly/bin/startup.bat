@echo off

rem set up environment for Java
rem set JAVA_HOME=D:\java\jdk1.8.0_202

for %%F in ("%~dp0\..\") do set "DBS_HOME=%%~dpF"
echo DBS_HOME=%DBS_HOME%
cd ../

if "%JAVA_HOME%"=="" (
  echo ERROR: JAVA_HOME is not set. Please set JAVA_HOME to your JDK 8 installation.
  exit /b 1
)

set "JAVA_EXT_DIR=%JAVA_HOME%\jre\lib\ext"
if not exist "%JAVA_EXT_DIR%" set "JAVA_EXT_DIR=%JAVA_HOME%\lib\ext"
if not exist "%JAVA_EXT_DIR%" (
  echo ERROR: Cannot find JRE lib\ext under JAVA_HOME=%JAVA_HOME%
  exit /b 1
)
echo JAVA_EXT_DIR=%JAVA_EXT_DIR%

set SERVER_OPTS=-Xms3800m -Xmx3800m -Xmn1500m -Xss512k -XX:MetaspaceSize=192m -XX:+DisableAttachMechanism
rem set IPv4
rem set SERVER_OPTS=%SERVER_OPTS% -Djava.net.preferIPv4Stack=true -Djava.net.preferIPv4Addresses

set ENCRYPT_FILE=%DBS_HOME%bin\libDBSyncer.dll
if exist %ENCRYPT_FILE% (
set SERVER_OPTS=%SERVER_OPTS% -agentpath:%ENCRYPT_FILE%
)

set SERVER_OPTS=%SERVER_OPTS% -Djava.ext.dirs="%JAVA_EXT_DIR%;%DBS_HOME%lib"
set SERVER_OPTS=%SERVER_OPTS% -Dspring.config.location=%DBS_HOME%conf\application.properties
set SERVER_OPTS=%SERVER_OPTS% -DLOG_PATH=%DBS_HOME%\logs
set SERVER_OPTS=%SERVER_OPTS% -Dsun.stdout.encoding=UTF-8 -Dfile.encoding=UTF-8 -Duser.dir=%DBS_HOME%
set SERVER_OPTS=%SERVER_OPTS% -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:ParallelGCThreads=4 -XX:+CMSClassUnloadingEnabled -XX:+DisableExplicitGC
set SERVER_OPTS=%SERVER_OPTS% -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=68 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps
set SERVER_OPTS=%SERVER_OPTS% -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%DBS_HOME%logs -XX:ErrorFile=%DBS_HOME%logs\hs_err.log

echo %SERVER_OPTS%
java %SERVER_OPTS% org.dbsyncer.web.Application