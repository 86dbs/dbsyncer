#!/bin/bash

# 获取目录路径
SCRIPT_DIR=$(cd "$(dirname "$0")"; pwd)
DBS_HOME=$(cd "$SCRIPT_DIR/.."; pwd)
echo "DBS_HOME=$DBS_HOME"

# 检查 JAVA_HOME
if [ -z "$JAVA_HOME" ] || [ ! -d "$JAVA_HOME" ]; then
  echo "Error: JAVA_HOME is not set or invalid: $JAVA_HOME" >&2
  exit 1
fi

# 检查 java 可执行文件
if [ ! -f "$JAVA_HOME/bin/java" ]; then
  echo "Error: java executable not found in $JAVA_HOME" >&2
  exit 1
fi

# 确保 conf 目录存在
if [ ! -d "$DBS_HOME/conf" ]; then
  mkdir -p "$DBS_HOME/conf"
fi

# 确保 logs 目录存在 (如果应用还会写文件日志，或者为了 GC 日志)
if [ ! -d "$DBS_HOME/logs" ]; then
  mkdir -p "$DBS_HOME/logs"
fi

# application.properties
CONFIG_PATH="$DBS_HOME/conf/application.properties"
if [ ! -f "$CONFIG_PATH" ]; then
  echo "The conf/application.properties doesn't exist, please check it first!" >&2
  exit 1
fi

###########################################################################
# 构建 JVM 参数
SERVER_OPTS="-Xms3800m -Xmx3800m -Xss512k -XX:MetaspaceSize=192m -XX:+DisableAttachMechanism"

ENCRYPT_FILE=''
if [[ $(uname -m) == "aarch64"* ]]; then
   ENCRYPT_FILE='libDBSyncer_aarch64.so'
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
   ENCRYPT_FILE='libDBSyncer.so'
elif [[ "$OSTYPE" == "darwin"* ]]; then
   ENCRYPT_FILE='libDBSyncer.dylib'
else
  echo "Unsupported OS." >&2
  exit 1
fi

if [ -e "$DBS_HOME/bin/$ENCRYPT_FILE" ]; then
  SERVER_OPTS="$SERVER_OPTS -agentpath:$DBS_HOME/bin/$ENCRYPT_FILE"
fi

# JVM 参数
SERVER_OPTS="$SERVER_OPTS \
-Djava.ext.dirs=$JAVA_HOME/jre/lib/ext:$DBS_HOME/lib \
-Dspring.config.location=$CONFIG_PATH \
-DLOG_HOME=$DBS_HOME/logs \
-Dsun.stdout.encoding=UTF-8 -Dfile.encoding=UTF-8 -Duser.dir=$DBS_HOME \
-XX:+UseG1GC \
-XX:MaxGCPauseMillis=200 \
-XX:G1NewSizePercent=30 \
-XX:G1MaxNewSizePercent=50 \
-XX:InitiatingHeapOccupancyPercent=45 \
-XX:G1ReservePercent=15 \
-XX:G1MixedGCLiveThresholdPercent=85 \
-XX:G1HeapWastePercent=5 \
-XX:G1MixedGCCountTarget=4 \
-XX:G1OldCSetRegionThresholdPercent=5 \
-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps \
-XX:+HeapDumpOnOutOfMemoryError \
-XX:HeapDumpPath=$DBS_HOME/logs \
-XX:ErrorFile=$DBS_HOME/logs/hs_err_pid_%p.log"

# execute command
echo "Starting DBSyncer Application..."
echo "================================================================"
echo "$SERVER_OPTS"
echo "================================================================"

exec java $SERVER_OPTS "$APP"