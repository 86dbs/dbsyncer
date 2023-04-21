@echo off

echo "Clean Project ..."
call mvn clean -f pom.xml

echo "Build Project ..."
call mvn compile package -f pom.xml -D"maven.test.skip=true"

set CP_PATH=%~dp0
move %CP_PATH%dbsyncer-web\target\dbsyncer-*.zip %CP_PATH%

:exit