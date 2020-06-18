#!/bin/bash
cd ../
echo 'Starting up ...'
# current path
CURRENT_DIR=$(pwd);
# the path of application.properties
PRO_CONFIG_NAME=application.properties
PRO_CONFIG_PATH='conf/'$PRO_CONFIG_NAME
if [[ ! -f $PRO_CONFIG_PATH ]] 
then
   echo "The '"$PRO_CONFIG_NAME"' does't exist, please check it first!"; exit 1;
fi
# read port from the file of application.properties
HOST=`grep dbsyncer.server.ip $PRO_CONFIG_PATH|cut -d'=' -f2`
PORT=`grep dbsyncer.server.port $PRO_CONFIG_PATH|cut -d'=' -f2`
# check port is used
pIDa=`netstat -tln | grep $PORT`
if [ "$pIDa" != "" ]
then
   echo "The port '"$PORT"' already in use, please kill the process or modify the port first!"; exit 1;
fi
# check logs dir is exist, otherwise create it.
if [ ! -d "logs" ]
then
  mkdir logs;
fi
###########################################################################
# current system date
CURRENT_DATE=$(date +%Y%m%d)
# set up environment for Java
#JAVA_HOME=/opt/jdk1.8.0_121
PATH=$JAVA_HOME/bin
# #CLASSPATH=.;$JAVA_HOME/lib;$JAVA_HOME/lib/dt.jar;$JAVA_HOME/lib/tools.jar
SERVER_OPTS='-Xms1024m -Xmx1024m -Xss1m -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=256m'
# set debug model
#SERVER_OPTS="$SERVER_OPTS -Djava.compiler=NONE -Xnoagent -Xdebug -Xrunjdwp:transport=dt_socket,address=15005,server=y,suspend=n"
# set jmxremote args
JMXREMOTE_CONFIG_PATH="$CURRENT_DIR/conf/boot"
JMXREMOTE_HOSTNAME="-Djava.rmi.server.hostname=$HOST"
JMXREMOTE_PORT="-Dcom.sun.management.jmxremote.port=15099"
JMXREMOTE_SSL="-Dcom.sun.management.jmxremote.ssl=false"
JMXREMOTE_AUTH="-Dcom.sun.management.jmxremote.authenticate=true"
JMXREMOTE_ACCESS="-Dcom.sun.management.jmxremote.access.file=$JMXREMOTE_CONFIG_PATH/jmxremote.access"
JMXREMOTE_PASSWORD="-Dcom.sun.management.jmxremote.password.file=$JMXREMOTE_CONFIG_PATH/jmxremote.password"
# set jmxremote model
#SERVER_OPTS="$SERVER_OPTS $JMXREMOTE_HOSTNAME $JMXREMOTE_PORT $JMXREMOTE_SSL $JMXREMOTE_AUTH $JMXREMOTE_ACCESS $JMXREMOTE_PASSWORD"
echo $SERVER_OPTS
# log path
SERVER_LOGS=$CURRENT_DIR/logs/catalina.$CURRENT_DATE.txt
echo $SERVER_LOGS
# create pid for stop.sh
SERVER_PID=$CURRENT_DIR/tmp.pid
# execute commond
java $SERVER_OPTS \
-Dfile.encoding=utf8 \
-Djava.ext.dirs=$JAVA_HOME/jre/lib/ext:./plugins:./lib \
-Dspring.config.location=%cd%\conf\application.properties \
org.dbsyncer.web.Application \
> $SERVER_LOGS & echo $! > $SERVER_PID
echo 'Start successfully!'
exit 1
