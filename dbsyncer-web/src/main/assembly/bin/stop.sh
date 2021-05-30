#!/bin/bash
cd ../
echo 'Stopping'
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
DEF_PORT=`grep dbsyncer.server.port $PRO_CONFIG_PATH|cut -d'=' -f2`
# default port
# print result
function printSuccess(){
  echo 'Stop successfully!';
}
# kill default process
function killDefPort()
{
  # If there is a default port, kill it.
  pIDa=`netstat -tln | grep $DEF_PORT`
  if [ "$pIDa" != "" ]
  then
	kill `lsof -t -i:$DEF_PORT`;
	echo "Kill default service process.";
	printSuccess;
  else
	echo "There is no service process that can be closed!"
  fi
}
# check tmp.pid exist
PID='tmp.pid'
if [[ ! -f $PID ]] 
then
  killDefPort;
  exit 1;
fi
# kill the process by tmp.pid
kill `cat tmp.pid`;
rm -f tmp.pid;
printSuccess;