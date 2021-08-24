#!/bin/bash
SCRIPT_DIR=$(cd $(dirname $0);pwd)
APP_DIR=$(cd $SCRIPT_DIR/..;pwd)
# application.properties
CONFIG_PATH=$APP_DIR'/conf/application.properties'
if [ ! -f ${CONFIG_PATH} ];
then
echo "The conf/application.properties does't exist, please check it first!";
exit 1
fi

# kill thread
PID=$APP_DIR/tmp.pid
if [ -f ${PID} ];
then
  pkill -f $PID
  rm -f $PID
fi
sleep 0.3

# kill process
APP="org.dbsyncer.web.Application" 
PROCESS="`ps -ef|grep java|grep ${APP}|awk '{print $2}'`"
if [[ -n ${PROCESS} ]];
then
  for p in ${PROCESS};
  do
    echo $p
    kill ${p}
  done
  echo 'Stop successfully!';
  exit 1;
fi

echo 'The app already stopped.';