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
#JAVA_HOME=/opt/jdk1.8.0_202
SERVER_OPTS='-Xms3800m -Xmx3800m -Xmn1500m -Xss512k -XX:MetaspaceSize=192m -XX:+DisableAttachMechanism'
# set IPv4
#SERVER_OPTS="$SERVER_OPTS -Djava.net.preferIPv4Stack=true -Djava.net.preferIPv4Addresses"

ENCRYPT_FILE=''
if [[ $(uname -m) == "aarch64"* ]]; then
   ENCRYPT_FILE='libDBSyncer_aarch64.so'
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
   ENCRYPT_FILE='libDBSyncer.so'
elif [[ "$OSTYPE" == "darwin"* ]]; then
   ENCRYPT_FILE='libDBSyncer.dylib'
else
  echo "Unsupported OS."
  exit 1
fi

if [ -e "$DBS_HOME/bin/$ENCRYPT_FILE" ]; then
  SERVER_OPTS="$SERVER_OPTS -agentpath:$DBS_HOME/bin/$ENCRYPT_FILE"
fi

# 确保 logs 目录存在
if [ ! -d "$DBS_HOME/logs" ]; then
  mkdir -p "$DBS_HOME/logs"
fi

SERVER_OPTS="$SERVER_OPTS \
-Djava.ext.dirs=$JAVA_HOME/jre/lib/ext:$DBS_HOME/lib \
-Dspring.config.location=$CONFIG_PATH \
-DLOG_HOME=$DBS_HOME/logs \
-Dsun.stdout.encoding=UTF-8 -Dfile.encoding=UTF-8 -Duser.dir=$DBS_HOME \
-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:ParallelGCThreads=4 \
-XX:+CMSClassUnloadingEnabled -XX:+DisableExplicitGC -XX:+UseCMSInitiatingOccupancyOnly \
-XX:CMSInitiatingOccupancyFraction=68 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps \
-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$DBS_HOME/logs -XX:ErrorFile=$DBS_HOME/logs/hs_err_pid_%p.log"

# execute command
echo $SERVER_OPTS
java $SERVER_OPTS -Dfile.encoding=UTF-8 $APP