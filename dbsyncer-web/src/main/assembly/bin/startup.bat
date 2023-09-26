@echo off

rem set up environment for Java
rem set JAVA_HOME=D:\java\jdk1.8.0_40
rem set PATH=%JAVA_HOME%\bin;%JAVA_HOME%\jre\bin;
rem set CLASSPATH=.;%JAVA_HOME%\lib;%JAVA_HOME%\lib\dt.jar;%JAVA_HOME%\lib\tools.jar

cd ../
echo starting up ...
set CURRENT_DIR=%cd%
echo %CURRENT_DIR%

set SERVER_OPTS=-Xms1024m -Xmx1024m -Xss1m -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=256m
rem debug model
set SERVER_OPTS=%SERVER_OPTS% -Djava.compiler=NONE -Xnoagent -Xdebug -Xrunjdwp:transport=dt_socket,address=15005,server=y,suspend=n
rem set IPv4
rem set SERVER_OPTS=%SERVER_OPTS% -Djava.net.preferIPv4Stack=true -Djava.net.preferIPv4Addresses

rem set jmxremote args
set HOST=127.0.0.1
set JMXREMOTE_CONFIG_PATH=%CURRENT_DIR%\config
set JMXREMOTE_HOSTNAME=-Djava.rmi.server.hostname=%HOST%
set JMXREMOTE_PORT=-Dcom.sun.management.jmxremote.port=15099
set JMXREMOTE_SSL=-Dcom.sun.management.jmxremote.ssl=false
set JMXREMOTE_AUTH=-Dcom.sun.management.jmxremote.authenticate=true
set JMXREMOTE_ACCESS=-Dcom.sun.management.jmxremote.access.file=%JMXREMOTE_CONFIG_PATH%\jmxremote.access
set JMXREMOTE_PASSWORD=-Dcom.sun.management.jmxremote.password.file=%JMXREMOTE_CONFIG_PATH%\jmxremote.password
rem jmxremote model
rem SERVER_OPTS=%SERVER_OPTS% %JMXREMOTE_HOSTNAME% %JMXREMOTE_PORT% %JMXREMOTE_SSL% %JMXREMOTE_AUTH% %JMXREMOTE_ACCESS% %JMXREMOTE_PASSWORD%

echo %SERVER_OPTS%
java %SERVER_OPTS% -Dfile.encoding=GBK -Djava.ext.dirs=%JAVA_HOME%\jre\lib\ext;./lib -Dspring.config.location=%cd%\conf\application.properties org.dbsyncer.web.Application