#!/bin/bash

# 获取目录路径
SCRIPT_DIR=$(cd "$(dirname "$0")"; pwd)
DBS_HOME=$(cd "$SCRIPT_DIR/.."; pwd)
echo "DBS_HOME=$DBS_HOME"

# 自动检测同目录下的 jre8
if [[ -z "${JRE_HOME:-}" && -d "$DBS_HOME/jre8" ]]; then
  export JRE_HOME="$DBS_HOME/jre8"
  echo "Using embedded JRE8: $JRE_HOME"
fi

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
# JRE 扩展目录：须包含 sunjce_provider，否则 TLS 报 SunTls12RsaPremasterSecret KeyGenerator not available
resolve_jre_ext_dir() {
  # 优先使用 JRE_HOME
  if [[ -n "${JRE_HOME:-}" ]]; then
    if [[ -d "$JRE_HOME/lib/ext" ]]; then
      echo "$JRE_HOME/lib/ext"
      return
    fi
    if [[ -d "$JRE_HOME/jre/lib/ext" ]]; then
      echo "$JRE_HOME/jre/lib/ext"
      return
    fi
  fi

  # 回退：查找系统 Java
  local java_home_prop
  if ! command -v java >/dev/null 2>&1; then
    echo ""
    return
  fi
  java_home_prop=$(java -XshowSettings:properties -version 2>&1 | tr -d '\r' | sed -n 's/^ *java\.home = //p' | head -1)
  if [[ -n "$java_home_prop" && -d "$java_home_prop/lib/ext" ]]; then
    echo "$java_home_prop/lib/ext"
    return
  fi

  echo ""
}

JAVA_EXT_DIR=$(resolve_jre_ext_dir)
if [[ -z "$JAVA_EXT_DIR" ]]; then
  echo "ERROR: Cannot find JRE lib/ext (JCE required for SSL). Set JAVA_HOME or fix PATH to java." >&2
  exit 1
fi
echo "JAVA_EXT_DIR=$JAVA_EXT_DIR"

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

# 1. 内存与基础配置 (放在最前面)
JAVA_OPTS+=("-Xms3g")
JAVA_OPTS+=("-Xmx3g")
JAVA_OPTS+=("-Xss512k")
JAVA_OPTS+=("-XX:MetaspaceSize=256m")
JAVA_OPTS+=("-XX:MaxDirectMemorySize=512m")
JAVA_OPTS+=("-XX:+DisableAttachMechanism")

# 2. GC 配置
JAVA_OPTS+=("-XX:+UnlockExperimentalVMOptions")
JAVA_OPTS+=("-XX:+UseG1GC")
JAVA_OPTS+=("-XX:G1HeapRegionSize=16m")
JAVA_OPTS+=("-XX:MaxGCPauseMillis=200")
JAVA_OPTS+=("-XX:+HeapDumpOnOutOfMemoryError")
JAVA_OPTS+=("-XX:HeapDumpPath=$DBS_HOME/logs/heapdump.hprof")
JAVA_OPTS+=("-XX:ErrorFile=$DBS_HOME/logs/hs_err_pid_%p.log")
# GC 日志已关闭，需要时可取消下面三行注释
# JAVA_OPTS+=("-verbose:gc")
# JAVA_OPTS+=("-XX:+PrintGCDetails")
# JAVA_OPTS+=("-XX:+PrintGCDateStamps")

# 3. Agent (如果有)
if [ -e "$DBS_HOME/bin/$ENCRYPT_FILE" ]; then
  JAVA_OPTS+=("-agentpath:$DBS_HOME/bin/$ENCRYPT_FILE")
fi

# 4. 系统属性
JAVA_OPTS+=("-Djava.ext.dirs=$JAVA_EXT_DIR:$DBS_HOME/lib")
JAVA_OPTS+=("-Dspring.config.location=$CONFIG_PATH")
JAVA_OPTS+=("-DLOG_HOME=$DBS_HOME/logs")
JAVA_OPTS+=("-Dsun.stdout.encoding=UTF-8")
JAVA_OPTS+=("-Dfile.encoding=UTF-8")
JAVA_OPTS+=("-Duser.timezone=Asia/Shanghai")
JAVA_OPTS+=("-Duser.dir=$DBS_HOME")

# 5. 主类
APP="org.dbsyncer.web.Application"

# 确定 java 路径：优先 jre8，否则使用系统 java
if [[ -n "${JRE_HOME:-}" ]]; then
  if [[ -x "$JRE_HOME/bin/java" ]]; then
    JAVA_BIN="$JRE_HOME/bin/java"
  elif [[ -x "$JRE_HOME/jre/bin/java" ]]; then
    JAVA_BIN="$JRE_HOME/jre/bin/java"
  else
    JAVA_BIN="java"
  fi
else
  JAVA_BIN="java"
fi

# execute command
echo "Starting DBSyncer Application..."

nohup "$JAVA_BIN" "${JAVA_OPTS[@]}" "$APP" > /dev/null 2>&1 &
APP_PID=$!

# 保存 PID 并反馈结果
echo "$APP_PID" > "$DBS_HOME/tmp.pid"
echo "Start successfully! PID: $APP_PID"
echo "Note: Please check $DBS_HOME/logs for detailed output."