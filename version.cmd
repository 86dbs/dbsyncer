@echo off

set /p APP_VERSION=Please enter a new version number(1.2.3-RC_0413):
echo %APP_VERSION%

echo "Clean Project ..."
call mvn clean -f pom.xml

echo "Update version ..."
call mvn versions:set -DnewVersion=%APP_VERSION% -DprocessAllModules=true -DallowSnapshots=true -DgenerateBackupPoms=false
call mvn -N versions:update-child-modules
call mvn versions:commit

:exit