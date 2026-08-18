#!/bin/bash
SCRIPT_DIR=$(cd "$(dirname "$0")"; pwd)
DBS_HOME=$(cd "$SCRIPT_DIR/.."; pwd)
# application.properties
CONFIG_PATH="$DBS_HOME/conf/application.properties"
if [ ! -f "$CONFIG_PATH" ]; then
  echo "The conf/application.properties doesn't exist, please check it first!"
  exit 1
fi

# 仅停止本安装目录的实例，避免同机多实例互相误杀
APP="org.dbsyncer.web.Application"
list_instance_pids() {
  local pid cmdline rest marker
  marker="-Duser.dir=${DBS_HOME}"
  for pid in $(pgrep -f "${APP}" 2>/dev/null); do
    if [[ -r "/proc/${pid}/cmdline" ]]; then
      cmdline=$(tr '\0' ' ' < "/proc/${pid}/cmdline")
    else
      cmdline=$(ps -p "${pid}" -o args= 2>/dev/null || true)
    fi
    rest="${cmdline#*"${marker}"}"
    [[ "$rest" == "$cmdline" ]] && continue
    if [[ -z "$rest" || "$rest" == [[:space:]]* ]]; then
      echo "$pid"
    fi
  done
}

PIDS=$(list_instance_pids)
PID_FILE="$DBS_HOME/tmp.pid"
if [[ -z "$PIDS" && -f "$PID_FILE" ]]; then
  OLD_PID=$(tr -d ' \t\r\n' < "$PID_FILE")
  if [[ -n "$OLD_PID" ]] && kill -0 "$OLD_PID" 2>/dev/null; then
    PIDS="$OLD_PID"
  fi
fi

if [[ -z "$PIDS" ]]; then
  rm -f "$PID_FILE"
  echo "The app already stopped."
  exit 0
fi

for p in $PIDS; do
  echo "$p"
  kill "$p" 2>/dev/null || true
done
rm -f "$PID_FILE"
sleep 0.3
echo "Stop successfully!"
