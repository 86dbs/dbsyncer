@echo off

rem set up environment for Java
rem set JAVA_HOME=D:\java\jdk1.8.0_202
rem set PATH=%JAVA_HOME%\bin;%JAVA_HOME%\jre\bin;
rem set CLASSPATH=.;%JAVA_HOME%\lib;%JAVA_HOME%\lib\dt.jar;%JAVA_HOME%\lib\tools.jar

for %%F in ("%~dp0\..\") do set "DBS_HOME=%%~dpF"
echo DBS_HOME=%DBS_HOME%
cd ../

set SERVER_OPTS=-Xms3800m -Xmx3800m -Xmn1500m -Xss512k -XX:MetaspaceSize=192m -XX:+DisableAttachMechanism
rem debug model
rem set SERVER_OPTS=%SERVER_OPTS% -Djava.compiler=NONE -Xnoagent -Xdebug -Xrunjdwp:transport=dt_socket,address=15005,server=y,suspend=n
rem set IPv4
rem set SERVER_OPTS=%SERVER_OPTS% -Djava.net.preferIPv4Stack=true -Djava.net.preferIPv4Addresses

rem set jmxremote args
set JMXREMOTE_HOSTNAME=-Djava.rmi.server.hostname=127.0.0.1
set JMXREMOTE_PORT=-Dcom.sun.management.jmxremote.port=15099
set JMXREMOTE_SSL=-Dcom.sun.management.jmxremote.ssl=false
set JMXREMOTE_AUTH=-Dcom.sun.management.jmxremote.authenticate=true
set JMXREMOTE_ACCESS=-Dcom.sun.management.jmxremote.access.file=%DBS_HOME%conf\jmxremote.access
set JMXREMOTE_PASSWORD=-Dcom.sun.management.jmxremote.password.file=%DBS_HOME%conf\jmxremote.password
rem jmxremote model
rem set SERVER_OPTS=%SERVER_OPTS% %JMXREMOTE_HOSTNAME% %JMXREMOTE_PORT% %JMXREMOTE_SSL% %JMXREMOTE_AUTH% %JMXREMOTE_ACCESS% %JMXREMOTE_PASSWORD%

set ENCRYPT_FILE=%DBS_HOME%bin\libDBSyncer.dll
if exist %ENCRYPT_FILE% (
set SERVER_OPTS=%SERVER_OPTS% -agentpath:%ENCRYPT_FILE%
)

set SERVER_OPTS=%SERVER_OPTS% -Djava.ext.dirs="%JAVA_HOME%\jre\lib\ext;%DBS_HOME%lib"
set SERVER_OPTS=%SERVER_OPTS% -Dspring.config.location=%DBS_HOME%conf\application.properties
set SERVER_OPTS=%SERVER_OPTS% -DLOG_PATH=%DBS_HOME%\logs
set SERVER_OPTS=%SERVER_OPTS% -Dsun.stdout.encoding=UTF-8 -Dfile.encoding=UTF-8 -Duser.dir=%DBS_HOME%
set SERVER_OPTS=%SERVER_OPTS% -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:ParallelGCThreads=4 -XX:+CMSClassUnloadingEnabled -XX:+DisableExplicitGC
set SERVER_OPTS=%SERVER_OPTS% -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=68 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps
set SERVER_OPTS=%SERVER_OPTS% -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%DBS_HOME%logs -XX:ErrorFile=%DBS_HOME%logs\hs_err.log

echo %SERVER_OPTS%
java %SERVER_OPTS% org.dbsyncer.web.Application