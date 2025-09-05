@echo off

set CURRENT_DATE=%date:~5,2%%date:~8,2%
set VERSION=2.3.1_%CURRENT_DATE%
set /p APP_VERSION=Please enter a new version number(%VERSION%): || set APP_VERSION=%VERSION%
echo %APP_VERSION%

echo "Clean Project ..."
call mvn clean -f pom.xml

echo "Update version ..."
call mvn versions:set -DnewVersion=%APP_VERSION% -DprocessAllModules=true -DallowSnapshots=true -DgenerateBackupPoms=false
call mvn -N versions:update-child-modules
call mvn versions:commit

:exit