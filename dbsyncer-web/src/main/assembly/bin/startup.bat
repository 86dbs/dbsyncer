@echo off

for %%F in ("%~dp0\..\") do set "DBS_HOME=%%~dpF"
echo DBS_HOME=%DBS_HOME%
cd ../

rem 自动检测同目录下的 jre8
if not defined JRE_HOME (
  if exist "%DBS_HOME%jre8" (
    set "JRE_HOME=%DBS_HOME%jre8"
    echo Using embedded JRE8: %JRE_HOME%
  )
)

rem 确定 java 路径：优先 jre8，否则使用系统 java
if defined JRE_HOME (
  if exist "%JRE_HOME%\bin\java.exe" (
    set "JAVA_BIN=%JRE_HOME%\bin\java.exe"
  ) else if exist "%JRE_HOME%\jre\bin\java.exe" (
    set "JAVA_BIN=%JRE_HOME%\jre\bin\java.exe"
  ) else (
    set "JAVA_BIN=java"
  )
) else (
  set "JAVA_BIN=java"
)

rem 确定 JAVA_EXT_DIR
if defined JRE_HOME (
  if exist "%JRE_HOME%\lib\ext" (
    set "JAVA_EXT_DIR=%JRE_HOME%\lib\ext"
  ) else if exist "%JRE_HOME%\jre\lib\ext" (
    set "JAVA_EXT_DIR=%JRE_HOME%\jre\lib\ext"
  )
) else (
  rem 回退：尝试通过系统 java 查找
  for /f "delims=" %%i in ('where java 2^>nul') do set "JAVA_PATH=%%~dpi"
  if defined JAVA_PATH (
    set "JAVA_EXT_DIR=%JAVA_PATH%jre\lib\ext"
    if not exist "%JAVA_EXT_DIR%" set "JAVA_EXT_DIR=%JAVA_PATH%lib\ext"
  )
)

if not exist "%JAVA_EXT_DIR%" (
  echo ERROR: Cannot find JRE lib\ext. Please ensure JRE8 is installed or set JRE_HOME environment variable. >&2
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
set SERVER_OPTS=%SERVER_OPTS% -Dsun.stdout.encoding=UTF-8 -Dfile.encoding=UTF-8 -Duser.timezone=Asia/Shanghai -Duser.dir=%DBS_HOME%
set SERVER_OPTS=%SERVER_OPTS% -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:ParallelGCThreads=4 -XX:+CMSClassUnloadingEnabled -XX:+DisableExplicitGC
set SERVER_OPTS=%SERVER_OPTS% -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=68 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps
set SERVER_OPTS=%SERVER_OPTS% -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%DBS_HOME%logs -XX:ErrorFile=%DBS_HOME%logs\hs_err.log

echo %SERVER_OPTS%
"%JAVA_BIN%" %SERVER_OPTS% org.dbsyncer.web.Application