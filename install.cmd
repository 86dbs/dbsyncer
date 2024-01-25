@echo off

echo "Install ..."
call mvn install -Dmaven.test.skip=true

:exit