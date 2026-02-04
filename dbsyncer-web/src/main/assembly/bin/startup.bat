@echo off

rem set up environment for Java
rem set JAVA_HOME=D:\java\jdk1.8.0_202

for %%F in ("%~dp0\..\") do set "DBS_HOME=%%~dpF"
echo DBS_HOME=%DBS_HOME%
cd ../

set SERVER_OPTS=-Xms3800m -Xmx3800m -Xmn1500m -Xss512k -XX:MetaspaceSize=192m -XX:+DisableAttachMechanism
rem set IPv4
rem set SERVER_OPTS=%SERVER_OPTS% -Djava.net.preferIPv4Stack=true -Djava.net.preferIPv4Addresses

set ENCRYPT_FILE=%DBS_HOME%bin\libDBSyncer.dll
if exist %ENCRYPT_FILE% (
set SERVER_OPTS=%SERVER_OPTS% -agentpath:%ENCRYPT_FILE%
)

set SERVER_OPTS=%SERVER_OPTS% -Djava.ext.dirs="%JAVA_HOME%\jre\lib\ext;%DBS_HOME%lib"
set SERVER_OPTS=%SERVER_OPTS% -Dspring.config.location=%DBS_HOME%conf\application.properties
set SERVER_OPTS=%SERVER_OPTS% -DLOG_PATH=%DBS_HOME%\logs
set SERVER_OPTS=%SERVER_OPTS% -Dsun.stdout.encoding=UTF-8 -Dfile.encoding=UTF-8 -Duser.dir=%DBS_HOME%
set SERVER_OPTS=%SERVER_OPTS% -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:ParallelGCThreads=4 -XX:+CMSClassUnloadingEnabled -XX:+DisableExplicitGC
rem GC 日志已关闭，需要时可取消下一行
set SERVER_OPTS=%SERVER_OPTS% -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=68
rem set SERVER_OPTS=%SERVER_OPTS% -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps
set SERVER_OPTS=%SERVER_OPTS% -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%DBS_HOME%logs -XX:ErrorFile=%DBS_HOME%logs\hs_err.log

echo %SERVER_OPTS%
java %SERVER_OPTS% org.dbsyncer.web.Application