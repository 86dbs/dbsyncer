#!/bin/bash
SCRIPT_DIR=$(cd $(dirname $0);pwd)
DBS_HOME=$(cd $SCRIPT_DIR/..;pwd)
echo DBS_HOME=$DBS_HOME
# application.properties
CONFIG_PATH=$DBS_HOME'/conf/application.properties'
if [ ! -f ${CONFIG_PATH} ]; then
  echo "The conf/application.properties does't exist, please check it first!";
  exit 1
fi

# check process
APP="org.dbsyncer.web.Application" 
PROCESS="`ps -ef|grep java|grep ${APP}|awk '{print $2}'`"
if [[ -n ${PROCESS} ]]; then
  echo "The app already started.";
  exit 1
fi

###########################################################################
# set up environment for Java
#JAVA_HOME=/opt/jdk1.8.0_121
PATH=$JAVA_HOME/bin
# #CLASSPATH=.;$JAVA_HOME/lib;$JAVA_HOME/lib/dt.jar;$JAVA_HOME/lib/tools.jar
SERVER_OPTS='-Xms3800m -Xmx3800m -Xmn1500m -Xss512k -XX:MetaspaceSize=192m'
# set debug model
#SERVER_OPTS="$SERVER_OPTS -Djava.compiler=NONE -Xnoagent -Xdebug -Xrunjdwp:transport=dt_socket,address=15005,server=y,suspend=n"
# set jmxremote args
JMXREMOTE_CONFIG_PATH="$DBS_HOME/conf"
JMXREMOTE_HOSTNAME="-Djava.rmi.server.hostname=$HOST"
JMXREMOTE_PORT="-Dcom.sun.management.jmxremote.port=15099"
JMXREMOTE_SSL="-Dcom.sun.management.jmxremote.ssl=false"
JMXREMOTE_AUTH="-Dcom.sun.management.jmxremote.authenticate=true"
JMXREMOTE_ACCESS="-Dcom.sun.management.jmxremote.access.file=$JMXREMOTE_CONFIG_PATH/jmxremote.access"
JMXREMOTE_PASSWORD="-Dcom.sun.management.jmxremote.password.file=$JMXREMOTE_CONFIG_PATH/jmxremote.password"
# set jmxremote model
#SERVER_OPTS="$SERVER_OPTS $JMXREMOTE_HOSTNAME $JMXREMOTE_PORT $JMXREMOTE_SSL $JMXREMOTE_AUTH $JMXREMOTE_ACCESS $JMXREMOTE_PASSWORD"
# set IPv4
#SERVER_OPTS="$SERVER_OPTS -Djava.net.preferIPv4Stack=true -Djava.net.preferIPv4Addresses"

ENCRYPT_FILE='libDBSyncer.so'
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
   ENCRYPT_FILE='libDBSyncer.so'
elif [[ "$OSTYPE" == "darwin"* ]]; then
   ENCRYPT_FILE='libDBSyncer.dylib'
else
    echo "Unsupported OS."
fi

if [ -e "$DBS_HOME/bin/$ENCRYPT_FILE" ]; then
  SERVER_OPTS="$SERVER_OPTS -agentpath:$DBS_HOME/bin/$ENCRYPT_FILE"
fi
SERVER_OPTS="$SERVER_OPTS \
-Djava.ext.dirs=$JAVA_HOME/jre/lib/ext:$DBS_HOME/lib \
-Dspring.config.location=$CONFIG_PATH \
-Dfile.encoding=UTF-8 -Duser.dir=$DBS_HOME \
-XX:+UseStringCache -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:ParallelGCThreads=4 \
-XX:+CMSClassUnloadingEnabled -XX:+DisableExplicitGC -XX:+UseCMSInitiatingOccupancyOnly \
-XX:CMSInitiatingOccupancyFraction=68 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps \
-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$DBS_HOME/logs -XX:ErrorFile=$DBS_HOME/logs/hs_err_pid_%p.log"

# execute command
echo $SERVER_OPTS
java $SERVER_OPTS $APP > /dev/null & echo $! > $DBS_HOME/tmp.pid
echo 'Start successfully!'
exit 1