#!/bin/bash

# 获取目录路径
SCRIPT_DIR=$(cd "$(dirname "$0")"; pwd)
DBS_HOME=$(cd "$SCRIPT_DIR/.."; pwd)
echo "DBS_HOME=$DBS_HOME"

# 确保 conf 目录存在
if [ ! -d "$DBS_HOME/conf" ]; then
  mkdir -p "$DBS_HOME/conf"
fi

# 确保 logs 目录存在
if [ ! -d "$DBS_HOME/logs" ]; then
  mkdir -p "$DBS_HOME/logs"
fi

# application.properties
CONFIG_PATH="$DBS_HOME/conf/application.properties"
if [ ! -f "$CONFIG_PATH" ]; then
  echo "The conf/application.properties doesn't exist, please check it first!"
  exit 1
fi

# check process
APP="org.dbsyncer.web.Application"
PROCESS=$(pgrep -f "${APP}")
if [[ -n "$PROCESS" ]]; then
  echo "The app already started (PID: $PROCESS)."
  exit 1
fi

###########################################################################
# 构建 JVM 参数
SERVER_OPTS="-Xms3800m -Xmx3800m -Xss512k -XX:MetaspaceSize=192m -XX:+DisableAttachMechanism"
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

# JVM 参数
SERVER_OPTS="$SERVER_OPTS \
-Djava.ext.dirs=$DBS_HOME/lib \
-Dspring.config.location=$CONFIG_PATH \
-DLOG_HOME=$DBS_HOME/logs \
-Dsun.stdout.encoding=UTF-8 -Dfile.encoding=UTF-8 -Duser.dir=$DBS_HOME \
-XX:+UseG1GC \
-XX:MaxGCPauseMillis=200 \
-XX:+UnlockExperimentalVMOptions \
-XX:G1NewSizePercent=30 \
-XX:G1MaxNewSizePercent=50 \
-XX:InitiatingHeapOccupancyPercent=45 \
-XX:G1ReservePercent=15 \
-XX:G1MixedGCLiveThresholdPercent=85 \
-XX:G1HeapWastePercent=5 \
-XX:G1MixedGCCountTarget=4 \
-XX:G1OldCSetRegionThresholdPercent=5 \
-XX:+HeapDumpOnOutOfMemoryError \
-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps \
-XX:HeapDumpPath=$DBS_HOME/logs \
-XX:ErrorFile=$DBS_HOME/logs/hs_err_pid_%p.log"

# execute command
echo "Starting DBSyncer Application..."
echo "================================================================"
echo "$SERVER_OPTS"
echo "================================================================"

nohup java $SERVER_OPTS "$APP" > /dev/null 2>&1 &
APP_PID=$!

# 保存 PID 并反馈结果
echo "$APP_PID" > "$DBS_HOME/tmp.pid"
echo "Start successfully! PID: $APP_PID"
echo "Note: Please check $DBS_HOME/logs for detailed output."