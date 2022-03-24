@echo off

for /F "eol=; tokens=2,2 delims==" %%i in ('findstr /i "info.app.version=" dbsyncer-web\src\main\resources\application.properties') DO set APP_VERSION=%%i
echo %APP_VERSION%

echo "Clean Project ..."
call mvn clean -f pom.xml

echo "Update version ..."
call mvn versions:set -DnewVersion=%APP_VERSION% -DprocessAllModules=true -DallowSnapshots=true -DgenerateBackupPoms=false
call mvn -N versions:update-child-modules 
call mvn versions:commit

:exit