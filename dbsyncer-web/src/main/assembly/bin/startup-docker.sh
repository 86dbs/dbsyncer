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
  echo "The conf/application.properties doesn't exist, please check it first!" >&2
  exit 1
fi

###########################################################################
# 构建 JVM 参数
JAVA_OPTS=()

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
  JAVA_OPTS+=("-agentpath:$DBS_HOME/bin/$ENCRYPT_FILE")
fi

# 1. 内存与基础配置 (放在最前面)
JAVA_OPTS+=("-Xms2g")
JAVA_OPTS+=("-Xmx2g")
JAVA_OPTS+=("-Xss512k")
JAVA_OPTS+=("-XX:MetaspaceSize=256m")
JAVA_OPTS+=("-XX:MaxDirectMemorySize=512m")
JAVA_OPTS+=("-XX:+DisableAttachMechanism")
JAVA_OPTS+=("-XX:NativeMemoryTracking=summary")

# 2. GC 配置
JAVA_OPTS+=("-XX:+UseG1GC")
JAVA_OPTS+=("-XX:G1HeapRegionSize=16m")
JAVA_OPTS+=("-XX:MaxGCPauseMillis=200")
JAVA_OPTS+=("-XX:+HeapDumpOnOutOfMemoryError")
JAVA_OPTS+=("-XX:HeapDumpPath=$DBS_HOME/logs/heapdump.hprof")
JAVA_OPTS+=("-XX:ErrorFile=$DBS_HOME/logs/hs_err_pid_%p.log")
JAVA_OPTS+=("-verbose:gc")
JAVA_OPTS+=("-XX:+PrintGCDetails")
JAVA_OPTS+=("-XX:+PrintGCDateStamps")

# 3. Agent (如果有)
if [ -e "$DBS_HOME/bin/$ENCRYPT_FILE" ]; then
  JAVA_OPTS+=("-agentpath:$DBS_HOME/bin/$ENCRYPT_FILE")
fi

# 4. 系统属性
JAVA_OPTS+=("-Djava.ext.dirs=$DBS_HOME/lib")
JAVA_OPTS+=("-Dspring.config.location=$CONFIG_PATH")
JAVA_OPTS+=("-DLOG_HOME=$DBS_HOME/logs")
JAVA_OPTS+=("-Dsun.stdout.encoding=UTF-8")
JAVA_OPTS+=("-Dfile.encoding=UTF-8")
JAVA_OPTS+=("-Duser.dir=$DBS_HOME")
# --- 注意：这里不要再加任何 -XX 参数了 ---

# 5. 主类 (放在最后)
APP="org.dbsyncer.web.Application"

###########################################################################
# execute command
echo "Starting DBSyncer Application..."
echo "================================================================"
# 打印参数数组
printf '%s ' "${JAVA_OPTS[@]}"
echo ""
echo "================================================================"

nohup java "${JAVA_OPTS[@]}" "$APP"